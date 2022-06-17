import pandas as pd
import itertools
import numpy as np
import datetime as dt
import pandas_gbq
from itertools import combinations
import warnings
from scipy.optimize import curve_fit
from calendar import monthrange
import os
import holidays

warnings.filterwarnings('ignore')

class AnyfinPortfolioForecast:
    
    def __init__(self, settings):
        
        self.fc_start_date = settings.fc_start_date
        self.fc_end_date_output = settings.fc_end_date
        self.SEK_USD_plan_rate = settings.SEK_USD_plan_rate
        self.SEK_EUR_plan_rate = settings.SEK_EUR_plan_rate
        self.EUR_USD_plan_rate = settings.EUR_USD_plan_rate
        self.included_countries = settings.included_countries
        self.load_new_data = settings.load_new_data
        self.creditor_split_new_customers = settings.creditor_split_new_customers
        self.forecast_version_id = str(dt.datetime.now()) #+ ' ' + os.getlogin()
        
        self.data_storage_path = os.getcwd() + os.sep + 'Data' + os.sep
        if os.path.exists(self.data_storage_path) == False:
            os.makedirs(self.data_storage_path)
        
        if self.fc_start_date.day == 1:            
            
            self.fc_first_full_month_start_date = self.fc_start_date
        
        else:
            
            self.fc_first_full_month_start_date = dt.datetime(
                (self.fc_start_date + pd.DateOffset(months = 1)).year,
                (self.fc_start_date + pd.DateOffset(months = 1)).month,
                1
                )
            
        self.fc_end_date = self.fc_end_date_output.replace(day = self.fc_end_date_output.days_in_month)
        
    def load_raw_data(self):
        
        country_string = ''

        for countries in self.included_countries:
            if countries != self.included_countries[len(self.included_countries)-1]:
                country_string = country_string + """'""" + countries + """', """
            else:
                country_string = country_string + """'""" + countries + """' """
        
        if self.load_new_data == True:
            
            print('Loading data from BigQuery...')

            self.raw_balance_data = pd.read_gbq(
                
                """
                
                    SELECT
                      dlb.date,
                      c.name AS creditor_id,
                      IF(l.type='annuity','annuity', 'flex') AS loan_type,
                      dlb.country_code,
                      dlb.currency_code,
                      DATE_TRUNC(dlb.loan_cohort, MONTH) AS loan_cohort,
                      dlb.months_since,
                      CASE
                          WHEN dlb.currency_code = 'SEK' AND dlb.principal_balance_excl_defaults > 100 THEN TRUE
                          WHEN dlb.currency_code = 'EUR' AND dlb.principal_balance_excl_defaults > 10 THEN TRUE
                          ELSE FALSE
                      END AS amount_significant,
                      SUM(dlb.principal_balance) AS principal_balance,
                      SUM(dlb.principal_balance_excl_defaults) AS principal_balance_excl_defaults,
                      SUM(dlb.revenue) AS revenue,
                      COUNT(dlb.loan_id) AS loan_count
                    FROM anyfin.finance.daily_loan_balances dlb
                    LEFT JOIN main.loans l ON l.id = dlb.loan_id
                    LEFT JOIN main.creditors c ON c.id = dlb.creditor_id
                    WHERE dlb.date < '""" + self.fc_start_date.strftime('%Y-%m-%d') + """' AND dlb.principal_balance_excl_defaults > 0 AND dlb.country_code IN (""" + country_string + """)
                    GROUP BY 1,2,3,4,5,6,7,8
                
                """,
                
                project_id = 'anyfin'
                
                )
            
            self.raw_balance_data['date'] = pd.to_datetime(self.raw_balance_data['date'], format='%Y-%m-%d')
            self.raw_balance_data['loan_cohort'] = pd.to_datetime(self.raw_balance_data['loan_cohort'], format='%Y-%m-%d')
            self.raw_balance_data.to_csv(self.data_storage_path + 'raw_balance_data.csv', index = False)
                        
            self.raw_origination_data = pd.read_gbq(
                
                """
                
                    SELECT
                      dlb.loan_cohort AS date,
                      DATE_TRUNC(dlb.loan_cohort, MONTH) AS loan_cohort,
                      IF(l.type='annuity','annuity', 'flex') AS loan_type,
                      dlb.country_code,
                      dlb.currency_code,
                      c.name AS creditor_id,
                      CASE
                        WHEN dlb.loan_cohort = dlb.customer_cohort THEN 'new_customer'
                        ELSE 'existing_customer'
                      END AS origination_type,
                      SUM(dlb.principal_balance) AS principal,
                      COUNT(dlb.loan_id) AS loan_count
                    FROM anyfin.finance.daily_loan_balances dlb
                    LEFT JOIN main.loans l ON l.id = dlb.loan_id
                    LEFT JOIN main.creditors c ON c.id = dlb.creditor_id
                    WHERE dlb.date = dlb.loan_cohort AND dlb.date < '""" + self.fc_start_date.strftime('%Y-%m-%d') + """' AND dlb.country_code IN (""" + country_string + """)
                    GROUP BY 1,2,3,4,5,6,7
                
                """,
                
                project_id = 'anyfin'
                
                )
            self.raw_origination_data['date'] = pd.to_datetime(self.raw_origination_data['date'], format='%Y-%m-%d')
            self.raw_origination_data['loan_cohort'] = pd.to_datetime(self.raw_origination_data['loan_cohort'], format='%Y-%m-%d')
            self.raw_origination_data.to_csv(self.data_storage_path + 'raw_origination_data.csv', index = False)
            
            self.raw_credit_risk_indicator_data = pd.read_gbq(
                
                """                
                
                    SELECT
                        EXTRACT(DAY FROM dlb.date) AS day_id,
                        dlb.country_code,
                        dlb.currency_code,
                        CASE
                          WHEN dlb.months_since <= 12 THEN CONCAT(CAST(dlb.months_since AS STRING),'M')
                          ELSE '>12M'
                        END AS age_group,
                        CASE
                          WHEN dlb.cpd >=4 THEN '90+'
                          WHEN dlb.cpd = cyb.cycles_since AND dlb.dpd >= 15 THEN 'missed_first_payment'
                          ELSE 'other'
                        END AS eligibility_type,
                        CASE
                          WHEN c.used_payment_vacation_at <= dlb.date THEN True
                          ELSE False
                        END AS on_holiday,
                        CASE
                            WHEN dlb.cpd = 0 THEN '0 cpd'
                            WHEN dlb.cpd = 1 THEN '1 ccpd'
                            WHEN dlb.cpd = 2 THEN '2 cpd'
                            WHEN dlb.cpd = 3 THEN '3 cpd'
                            WHEN dlb.cpd = 4 THEN '4 cpd'
                            ELSE '>4 cpd'
                        END AS cpd,
                        CASE
                          WHEN COALESCE(l.external_score, l2.external_score) < 10 THEN '0-10'
                          ELSE '>10'
                        END AS score_bucket,
                        SUM(dlb.principal_balance) AS principal_balance,
                        COUNT(dlb.loan_id) AS loan_count
                      FROM finance.daily_loan_balances dlb
                      LEFT JOIN main.cycles c ON c.loan_id = dlb.loan_id AND dlb.date BETWEEN c.start_date AND c.due_date
                      LEFT JOIN finance.loan_facts l on l.loan_id = dlb.loan_id
                      LEFT JOIN finance.loan_facts l2 on l2.source_loan_id = dlb.loan_id
                      LEFT JOIN finance.cycle_balances cyb ON cyb.loan_id = dlb.loan_id AND dlb.date BETWEEN cyb.start_date AND cyb.due_date AND dlb.date < '""" + self.fc_start_date.strftime('%Y-%m-%d') + """' AND dlb.country_code IN (""" + country_string + """)
                      WHERE dlb.principal_balance > 0
                      GROUP BY 1,2,3,4,5,6,7,8
                      
                
                """,
                
                project_id = 'anyfin'
                
                )
            
            self.raw_credit_risk_indicator_data.to_csv(self.data_storage_path + 'raw_credit_risk_indicator_data.csv', index = False)
            
        else:
            
            print('Loading data from local csv-file...')
            
            self.raw_balance_data = pd.read_csv(self.data_storage_path + 'raw_balance_data.csv', parse_dates = ['date', 'loan_cohort'])
            self.raw_origination_data = pd.read_csv(self.data_storage_path + 'raw_origination_data.csv', parse_dates = ['date', 'loan_cohort'])
            self.raw_credit_risk_indicator_data = pd.read_csv(self.data_storage_path + 'raw_credit_risk_indicator_data.csv')
            
        self.raw_balance_data['principal_balance'] = self.raw_balance_data['principal_balance'].astype(float)
        self.raw_balance_data['principal_balance_excl_defaults'] = self.raw_balance_data['principal_balance_excl_defaults'].astype(float)
        self.raw_balance_data['revenue'] = self.raw_balance_data['revenue'].astype(float)
        self.raw_balance_data['loan_count'] = self.raw_balance_data['loan_count'].astype(float)
        
        self.raw_origination_data['principal'] = self.raw_origination_data['principal'].astype(float)
        self.raw_origination_data['loan_count'] = self.raw_origination_data['loan_count'].astype(float)
        
        self.raw_credit_risk_indicator_data['principal_balance'] = self.raw_credit_risk_indicator_data['principal_balance'].astype(float)
        self.raw_credit_risk_indicator_data['loan_count'] = self.raw_credit_risk_indicator_data['loan_count'].astype(float)
            
    def generate_additional_datasets(self):
                
        date_list_df = pd.DataFrame().assign(date = pd.date_range(start = self.fc_start_date, end = self.fc_end_date))
        date_list = date_list_df['date'].to_list()
        market_list = self.raw_balance_data['country_code'].drop_duplicates().to_list()
        cohort_list = pd.concat([
            self.raw_balance_data['loan_cohort'].drop_duplicates(),
            pd.DataFrame().assign(date = pd.date_range(start = self.fc_start_date, end = self.fc_end_date))['date'].dt.to_period('M').dt.to_timestamp()
            ]).drop_duplicates().to_list()
        country_currency_pairs = self.raw_balance_data[['country_code', 'currency_code']].drop_duplicates()
        country_loan_type_pairs = self.raw_balance_data[['country_code', 'loan_type']].drop_duplicates()

        self.balance_output_template = pd.DataFrame(
            list(itertools.product(date_list, cohort_list, market_list)),
            columns = ['date', 'loan_cohort', 'country_code']).drop_duplicates().merge(
                country_currency_pairs,
                how = 'left',
                on = ['country_code']
                ).merge(
                    country_loan_type_pairs,
                    how = 'left',
                    on = ['country_code']
                    )
        self.balance_output_template = self.balance_output_template[self.balance_output_template['date'] >=  self.balance_output_template['loan_cohort']]
        self.balance_output_template = self.generate_date_and_age_attributes(self.balance_output_template, 'date', 'loan_cohort')
                        
        self.origination_output_template = pd.DataFrame(
            list(itertools.product(date_list, market_list)),
            columns = ['date', 'country_code']).drop_duplicates().merge(
                country_currency_pairs,
                how = 'left',
                on = ['country_code']
                ).merge(
                    country_loan_type_pairs,
                    how = 'left',
                    on = ['country_code']
                    )
        self.origination_output_template['loan_cohort'] = self.origination_output_template['date'].dt.to_period('M').dt.to_timestamp()
        self.origination_output_template = self.generate_date_and_age_attributes(self.origination_output_template, 'date', 'date')
        
    def date_is_holiday(self, date, country_code = None):
        
        if country_code not in ['SE', 'DE', 'FI', 'NO', 'ES']:
            
            return False
        
        if country_code == 'SE':
            holiday_list = holidays.Sweden()
        
        if country_code == 'DE':
            holiday_list = holidays.Germany()
            
        if country_code == 'FI':
            holiday_list = holidays.Finland()
            
        if country_code == 'NO':
            holiday_list = holidays.Norway()
            
        if country_code == 'ES':               
            holiday_list = holidays.Spain()
        
        if holiday_list.get(date) == None:
            
            return False
        
        else:
            
            return True
    
    def consider_national_holidays(self, df, date_column_name, country_code_column_name):
        
        df_dates = df[[date_column_name, country_code_column_name]].drop_duplicates()
        df_dates['is_holiday'] = df_dates.apply(lambda x: self.date_is_holiday(x[date_column_name], x[country_code_column_name]), axis = 1)
            
        df = df.merge(
            df_dates,
            how = 'left',
            on = [date_column_name, country_code_column_name]
            )
        
        df.loc[df['is_holiday'] == True, 'weekday_id'] = 'Weekend'
        
        return df.drop(['is_holiday'], axis = 1)
        
    def generate_date_and_age_attributes(self, df, date_column_name, cohort_column_name):
                
        # Cohort age buckets
        
        df['cohort_age_months'] = 12 * (df[date_column_name].dt.year - df[cohort_column_name].dt.year) + (df[date_column_name].dt.month - df[cohort_column_name].dt.month)
        bins = [-np.inf, 0, 1, 2, 3, 6, np.inf]
        labels = ['0M', '1M', '2M', '3M', '3-6M', '>6M']
        df['age_bucket'] = pd.cut(df['cohort_age_months'], bins = bins, labels = labels)

        # Day in month bucket
        
        bins = [-np.inf, 5, 10, 15, 20, 25, np.inf]
        labels = ['day 1-5', 'day 6-10', 'day 11-15', 'day 16-20', 'day 21-25', 'day 26-31']
        df['day_id'] = pd.cut(df[date_column_name].dt.day, bins = bins, labels = labels)

        # Day in week buckets
        
        bins = [-np.inf, 0, 4, np.inf]
        labels = ['Mon', 'Tue-Fri', 'Weekend']
        df['weekday_id'] = pd.cut(df[date_column_name].dt.weekday, bins = bins, labels = labels)
        
        # Treat national holidays like weekends
                
        df = self.consider_national_holidays(df, date_column_name, 'country_code')
        
        # Month in year bucket
        
        df['month_id'] = df[date_column_name].dt.month
        
        return df.drop(['cohort_age_months'], axis = 1).reset_index(drop = True)

    def estimate_balance_growth_factors(self):
        
        model_estimation_data = self.raw_balance_data[(self.raw_balance_data['amount_significant'] == True) & (self.raw_balance_data['loan_count'] > 20)][['date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type', 'principal_balance', 'loan_count']].groupby([
                'date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type'
                ]).sum().reset_index()
        model_estimation_data['balance_per_loan'] = model_estimation_data['principal_balance']/model_estimation_data['loan_count']        
        model_estimation_data = model_estimation_data.merge(
            model_estimation_data.assign(date = model_estimation_data.date + pd.DateOffset(days = 1)),
            how = 'left',
            on = ['date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type'],
            suffixes = ('', '_prev_day')
            )
        model_estimation_data = self.generate_date_and_age_attributes(model_estimation_data, 'date', 'loan_cohort')
        
        balance_growth_rates_1 = model_estimation_data.groupby(['country_code', 'currency_code', 'loan_type', 'month_id', 'day_id', 'age_bucket', 'weekday_id']).sum().reset_index()
        balance_growth_rates_1['balance_growth_rate'] = (balance_growth_rates_1['balance_per_loan']/balance_growth_rates_1['balance_per_loan_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_1['loan_growth_rate'] = (balance_growth_rates_1['loan_count']/balance_growth_rates_1['loan_count_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_1 = balance_growth_rates_1.drop(['balance_per_loan', 'balance_per_loan_prev_day', 'loan_count', 'loan_count_prev_day', 'principal_balance', 'principal_balance_prev_day'], axis = 1)
        
        balance_growth_rates_2 = model_estimation_data.groupby(['country_code', 'currency_code', 'loan_type', 'month_id', 'day_id', 'age_bucket']).sum().reset_index()
        balance_growth_rates_2['balance_growth_rate'] = (balance_growth_rates_2['balance_per_loan']/balance_growth_rates_2['balance_per_loan_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_2['loan_growth_rate'] = (balance_growth_rates_2['loan_count']/balance_growth_rates_2['loan_count_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_2 = balance_growth_rates_2.drop(['balance_per_loan', 'balance_per_loan_prev_day', 'loan_count', 'loan_count_prev_day', 'principal_balance', 'principal_balance_prev_day'], axis = 1)
   
        balance_growth_rates_3 = model_estimation_data.groupby(['country_code', 'currency_code', 'month_id', 'day_id', 'age_bucket']).sum().reset_index()
        balance_growth_rates_3['balance_growth_rate'] = (balance_growth_rates_3['balance_per_loan']/balance_growth_rates_3['balance_per_loan_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_3['loan_growth_rate'] = (balance_growth_rates_3['loan_count']/balance_growth_rates_3['loan_count_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_3 = balance_growth_rates_3.drop(['balance_per_loan', 'balance_per_loan_prev_day', 'loan_count', 'loan_count_prev_day', 'principal_balance', 'principal_balance_prev_day'], axis = 1)
        
        balance_growth_rates_4 = model_estimation_data.groupby(['country_code', 'currency_code', 'day_id', 'age_bucket']).sum().reset_index()
        balance_growth_rates_4['balance_growth_rate'] = (balance_growth_rates_4['balance_per_loan']/balance_growth_rates_4['balance_per_loan_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_4['loan_growth_rate'] = (balance_growth_rates_4['loan_count']/balance_growth_rates_4['loan_count_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_4 = balance_growth_rates_4.drop(['balance_per_loan', 'balance_per_loan_prev_day', 'loan_count', 'loan_count_prev_day', 'principal_balance', 'principal_balance_prev_day', 'month_id'], axis = 1)
        
        self.growth_rates = self.balance_output_template.merge(
            balance_growth_rates_1,
            how = 'left',
            on = ['country_code', 'currency_code', 'loan_type', 'month_id', 'day_id', 'age_bucket', 'weekday_id']            
            ).merge(
                balance_growth_rates_2,
                how = 'left',
                on = ['country_code', 'currency_code', 'loan_type', 'month_id', 'day_id', 'age_bucket'],
                suffixes = ('', '_backup')
                ).merge(
                    balance_growth_rates_3,
                    how = 'left',
                    on = ['country_code', 'currency_code', 'month_id', 'day_id', 'age_bucket'],
                    suffixes = ('', '_backup_2')
                    ).merge(
                        balance_growth_rates_4,
                        how = 'left',
                        on = ['country_code', 'currency_code', 'day_id', 'age_bucket'],
                        suffixes = ('', '_backup_3')
                        )
                    
        self.growth_rates['balance_growth_rate'] = self.growth_rates['balance_growth_rate'].fillna(self.growth_rates['balance_growth_rate_backup']).fillna(self.growth_rates['balance_growth_rate_backup_2']).fillna(self.growth_rates['balance_growth_rate_backup_3'])
        self.growth_rates['loan_growth_rate'] = self.growth_rates['loan_growth_rate'].fillna(self.growth_rates['loan_growth_rate_backup']).fillna(self.growth_rates['loan_growth_rate_backup_2']).fillna(self.growth_rates['loan_growth_rate_backup_3'])
        self.growth_rates = self.growth_rates.drop(['balance_growth_rate_backup', 'loan_growth_rate_backup', 'balance_growth_rate_backup_2', 'loan_growth_rate_backup_2', 'balance_growth_rate_backup_3', 'loan_growth_rate_backup_3'], axis = 1)
        
    def generate_backbook_forecast(self):
        
        starting_point = self.generate_date_and_age_attributes(
            self.raw_balance_data[(self.raw_balance_data['date']<self.fc_start_date) & (self.raw_balance_data['loan_cohort'] < dt.datetime(self.fc_start_date.year,self.fc_start_date.month,1))],
            'date',
            'loan_cohort'
            )[self.balance_output_template.columns.to_list() + ['creditor_id', 'principal_balance', 'loan_count']]                
        starting_point = starting_point[starting_point['date'] == starting_point['date'].max()]
        starting_point = starting_point.groupby(['date', 'loan_cohort', 'country_code', 'loan_type', 'creditor_id', 'currency_code']).sum().reset_index()
        starting_point['balance_per_loan'] = starting_point['principal_balance'] / starting_point['loan_count']
        starting_point['data_source'] = 'actual'

        print('Generating backbook forecast...')
        
        self.backbook_forecast = self.generate_new_days(starting_point, 'backbook_forecast')
        self.backbook_forecast = self.backbook_forecast[self.backbook_forecast['data_source'] != 'actual']
        
    def generate_new_days(self, days_up_until_yesterday, data_source_name):
        
        last_date = days_up_until_yesterday['date'].copy().max()

        if last_date == self.fc_end_date:
            
            return days_up_until_yesterday
        
        else:
            
            new_day_data = days_up_until_yesterday[days_up_until_yesterday['date'] == last_date]
            new_day_data['date'] = new_day_data['date'] + pd.DateOffset(days = 1)
            new_day_data = self.generate_date_and_age_attributes(
                new_day_data,
                'date',
                'loan_cohort'
                )
            
            new_day_data = new_day_data.merge(
                self.growth_rates,
                how = 'left',
                on = self.balance_output_template.columns.to_list()
                )
            new_day_data['balance_per_loan'] *= new_day_data['balance_growth_rate']
            new_day_data['loan_count'] *= new_day_data['loan_growth_rate']
            new_day_data['principal_balance'] = new_day_data['balance_per_loan']*new_day_data['loan_count']        
            new_day_data = new_day_data.drop(['balance_growth_rate', 'loan_growth_rate'], axis = 1)
            new_day_data['data_source'] = data_source_name
            
            return self.generate_new_days(pd.concat([days_up_until_yesterday, new_day_data]), data_source_name)
        
    def generate_first_month_balance_to_origination_ratios(self):
        
        first_month_balances = self.raw_balance_data[(self.raw_balance_data['months_since'] == 0)][['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code', 'principal_balance', 'loan_count']].groupby(['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code']).sum().reset_index()
        cumulative_origination_data = self.raw_origination_data.groupby(['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code']).sum().sort_values(by='date').groupby(['loan_cohort', 'loan_type', 'country_code', 'currency_code']).cumsum().reset_index()
        
        model_estimation_data = first_month_balances.merge(
            cumulative_origination_data.rename(columns = {'principal':'cum_originated_amount', 'loan_count':'cum_loan_count'}),
            how = 'left',
            on = ['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code']
            ).sort_values(by='date')        
        model_estimation_data['cum_originated_amount'] = model_estimation_data.groupby(['loan_cohort', 'loan_type', 'country_code', 'currency_code'])['cum_originated_amount'].transform(lambda v: v.ffill())
        model_estimation_data['cum_loan_count'] = model_estimation_data.groupby(['loan_cohort', 'loan_type', 'country_code', 'currency_code'])['cum_loan_count'].transform(lambda v: v.ffill())        
        model_estimation_data['day_id'] = model_estimation_data['date'].dt.day
        
        self.balance_to_origination_ratios = model_estimation_data.groupby(['loan_type', 'country_code', 'currency_code', 'day_id']).sum().reset_index()
        self.balance_to_origination_ratios['cum_balance_ratio'] = self.balance_to_origination_ratios['principal_balance']/self.balance_to_origination_ratios['cum_originated_amount']
        self.balance_to_origination_ratios['cum_loan_ratio'] = self.balance_to_origination_ratios['loan_count']/self.balance_to_origination_ratios['cum_loan_count']        
        self.balance_to_origination_ratios = self.balance_to_origination_ratios.drop(['loan_count', 'cum_loan_count', 'principal_balance', 'cum_originated_amount'], axis = 1)
        
    def generate_new_vs_old_customer_split(self):
        
        template = self.raw_origination_data[['date', 'country_code', 'currency_code', 'loan_type']].drop_duplicates()
        new_customer_obs = self.raw_origination_data[self.raw_origination_data['origination_type'] == 'new_customer'][['date', 'country_code', 'currency_code', 'loan_type', 'loan_count']].groupby([
            'date', 'country_code', 'currency_code', 'loan_type'
            ]).sum().reset_index()
        all_customer_obs = self.raw_origination_data[['date', 'country_code', 'currency_code', 'loan_type', 'loan_count']].groupby([
            'date', 'country_code', 'currency_code', 'loan_type'
            ]).sum().reset_index()
        
        customer_split_obs = template.merge(
                all_customer_obs,
                how = 'left',
                on = ['date', 'country_code', 'currency_code', 'loan_type']
            ).merge(
                new_customer_obs,
                how = 'left',
                on = ['date', 'country_code', 'currency_code', 'loan_type'],
                suffixes = ('', '_new')
            )
        customer_split_obs['new_customer_share'] = (customer_split_obs['loan_count_new'] / customer_split_obs['loan_count']).replace([-np.inf, np.inf], np.nan).fillna(0)
        customer_split_obs = customer_split_obs.drop(['loan_count', 'loan_count_new'], axis = 1)
        
        self.customer_split_fc = self.origination_output_template[['date', 'country_code', 'currency_code', 'loan_type', 'loan_cohort']].drop_duplicates()
        
        for markets in customer_split_obs['country_code'].drop_duplicates().to_list():
            for loan_type in customer_split_obs['loan_type'].drop_duplicates().to_list():
                
                customer_split_obs_i = customer_split_obs[(customer_split_obs['country_code'] == markets) & (customer_split_obs['loan_type'] == loan_type)]
                base_date_i = customer_split_obs_i['date'].min()
                customer_split_obs_i['base_date'] = base_date_i
                customer_split_obs_i['t'] = (customer_split_obs_i['date'] - customer_split_obs_i['base_date']).dt.days
                
                a_i, b_i = curve_fit(
                    
                    self.existing_customer_share_function,
                    customer_split_obs_i['t'].to_list(),
                    customer_split_obs_i['new_customer_share'].to_list(),
                    bounds = [(0,0), (np.inf,np.inf)]
                    
                    )[0]
                
                self.customer_split_fc.loc[
                    (self.customer_split_fc['country_code'] == markets) &
                    (self.customer_split_fc['loan_type'] == loan_type),
                    'a'
                    ] = a_i
                
                self.customer_split_fc.loc[
                    (self.customer_split_fc['country_code'] == markets) &
                    (self.customer_split_fc['loan_type'] == loan_type),
                    'b'
                    ] = b_i
                
                self.customer_split_fc.loc[
                    (self.customer_split_fc['country_code'] == markets) &
                    (self.customer_split_fc['loan_type'] == loan_type),
                    'base_date'
                    ] = base_date_i
                
        self.customer_split_fc['t'] = (self.customer_split_fc['date'] - self.customer_split_fc['base_date']).dt.days                
        self.customer_split_fc = self.customer_split_fc.assign(new_customer_share = lambda x: self.existing_customer_share_function(x['t'], x['a'], x['b']))
        self.customer_split_fc = self.customer_split_fc.drop(['a', 'b', 't', 'base_date'], axis = 1)
        
    def existing_customer_share_function(self, t, a, b):
        
        return a/(a+b*t)
    
    def generate_creditor_distribution_new_customers(self):
        
        date_list = self.origination_output_template['date'].drop_duplicates().to_list()
        country_list = self.origination_output_template['country_code'].drop_duplicates().to_list()        
        country_creditor_pairs = self.creditor_split_new_customers[['Country', 'Creditor']].drop_duplicates()
        
        self.creditor_split_fc = pd.DataFrame(
            list(itertools.product(date_list, country_list)),
            columns = ['date', 'country_code']).drop_duplicates().merge(
                country_creditor_pairs.rename(columns = {'Country':'country_code', 'Creditor': 'creditor_id'}),
                how = 'left',
                on = ['country_code']
                )
        
        for creditors in self.creditor_split_new_customers['Creditor'].drop_duplicates().to_list():
            
            for dates in self.creditor_split_new_customers[self.creditor_split_new_customers['Creditor'] == creditors].sort_values(by = 'Apply from date')['Apply from date'].drop_duplicates().to_list():
                
                country_code_ij = self.creditor_split_new_customers[
                    
                    (self.creditor_split_new_customers['Creditor'] == creditors) &
                    (self.creditor_split_new_customers['Apply from date'] == dates)
                    ]['Country'].values[0]
                
                creditor_split_ij = self.creditor_split_new_customers[
                    (self.creditor_split_new_customers['Creditor'] == creditors) &
                    (self.creditor_split_new_customers['Apply from date'] == dates)
                    ]['Share of new customer loans'].values[0] 
                
                self.creditor_split_fc.loc[
                    
                    (self.creditor_split_fc['country_code'] == country_code_ij) &
                    (self.creditor_split_fc['creditor_id'] == creditors) &
                    (self.creditor_split_fc['date'] >= dates),
                    'creditor_share_new_customers'
                    ] = creditor_split_ij
                
        self.creditor_split_fc['creditor_share_new_customers'] = self.creditor_split_fc['creditor_share_new_customers'].fillna(0)
        self.creditor_split_fc = self.creditor_split_fc[self.creditor_split_fc['creditor_share_new_customers']>0]
    
    def generate_monthly_origination_growth_rates(self):
        
        model_monthly_estimation_data = self.raw_origination_data.groupby(['loan_cohort', 'country_code', 'currency_code']).sum().reset_index()
        model_monthly_estimation_data = model_monthly_estimation_data[(model_monthly_estimation_data['loan_cohort']>self.fc_start_date - pd.DateOffset(months = 14)) & (model_monthly_estimation_data['loan_count'] > 1000)]        
        model_monthly_estimation_data = model_monthly_estimation_data[model_monthly_estimation_data['loan_cohort'] < self.fc_start_date.replace(day = 1)]
        model_monthly_estimation_data = model_monthly_estimation_data.merge(
            model_monthly_estimation_data.assign(loan_cohort = model_monthly_estimation_data.loan_cohort + pd.DateOffset(months = 1)),
            how = 'left',
            on = ['loan_cohort', 'country_code', 'currency_code'],
            suffixes = ('', '_last_month')
            )
        model_monthly_estimation_data['month_id'] = model_monthly_estimation_data['loan_cohort'].dt.month
        
        monthly_growth_1 = model_monthly_estimation_data.groupby(['month_id', 'country_code', 'currency_code']).sum().reset_index()
        monthly_growth_1['loan_growth'] = (monthly_growth_1['loan_count']/monthly_growth_1['loan_count_last_month'])
        monthly_growth_1['balance_growth'] = (monthly_growth_1['principal']/monthly_growth_1['loan_count'])/(monthly_growth_1['principal_last_month']/monthly_growth_1['loan_count_last_month'])
        monthly_growth_1 = monthly_growth_1.drop(['principal', 'loan_count', 'principal_last_month', 'loan_count_last_month'], axis = 1)
        
        monthly_growth_2 = model_monthly_estimation_data.groupby(['country_code', 'currency_code']).sum().reset_index().drop(['month_id'], axis = 1)
        monthly_growth_2['loan_growth'] = (monthly_growth_2['loan_count']/monthly_growth_2['loan_count_last_month'])
        monthly_growth_2['balance_growth'] = (monthly_growth_2['principal']/monthly_growth_2['loan_count'])/(monthly_growth_2['principal_last_month']/monthly_growth_2['loan_count_last_month'])
        monthly_growth_2 = monthly_growth_2.drop(['principal', 'loan_count', 'principal_last_month', 'loan_count_last_month'], axis = 1)
        
        self.origination_monthly_growth_rates = self.origination_output_template[['loan_cohort', 'country_code', 'currency_code', 'loan_type', 'month_id']].drop_duplicates().merge(
            monthly_growth_1,
            how = 'left',
            on = ['month_id', 'country_code', 'currency_code']
            ).merge(
                monthly_growth_2,
                how = 'left',
                on = ['country_code', 'currency_code'],
                suffixes = ('', '_2')
                )
                
        self.origination_monthly_growth_rates['balance_growth'] = self.origination_monthly_growth_rates['balance_growth'].replace([-np.inf, np.inf], np.nan).fillna(self.origination_monthly_growth_rates['balance_growth_2']).replace([-np.inf, np.inf], np.nan).fillna(1)
        self.origination_monthly_growth_rates['loan_growth'] = self.origination_monthly_growth_rates['loan_growth'].replace([-np.inf, np.inf], np.nan).fillna(self.origination_monthly_growth_rates['loan_growth_2']).replace([-np.inf, np.inf], np.nan).fillna(1)
        self.origination_monthly_growth_rates = self.origination_monthly_growth_rates.drop(['balance_growth_2', 'loan_growth_2'], axis = 1)
        
    def generate_monthly_origination_forecast(self):
        
        print('Generating monthly origination forecast...')
        
        starting_point = self.raw_origination_data[self.raw_origination_data['loan_cohort'] < self.fc_start_date.replace(day = 1)]
        starting_point = starting_point.groupby(['loan_cohort', 'loan_type', 'country_code', 'currency_code']).sum().reset_index()
        starting_point['principal_per_loan'] = starting_point['principal']/starting_point['loan_count']
        starting_point['data_source'] = 'actual'
        
        self.monthly_origination_forecast = self.generate_future_origination_months(starting_point)
        self.monthly_origination_forecast = self.monthly_origination_forecast.drop(['principal_per_loan'], axis = 1)
                        
    def estimate_intra_month_origination_profile(self):
        
        daily_origination = self.raw_origination_data[['date', 'loan_cohort', 'loan_type', 'country_code', 'loan_count']].groupby(['date', 'loan_cohort', 'loan_type', 'country_code']).sum().reset_index()
        monthly_origination = self.raw_origination_data[['date', 'loan_cohort', 'loan_type', 'country_code', 'loan_count']].groupby(['loan_cohort', 'loan_type', 'country_code']).sum().reset_index()

        monthly_profiles = self.generate_date_and_age_attributes(
            daily_origination.merge(
                monthly_origination,
                how = 'left',
                on = ['loan_cohort', 'loan_type', 'country_code'],
                suffixes = ('', '_month_tot')
                ),
            'date',
            'loan_cohort'
            )
        
        monthly_profiles['monthly_share'] = monthly_profiles['loan_count'] / monthly_profiles['loan_count_month_tot']
        monthly_profiles['weighted_monthly_share_i'] = monthly_profiles['monthly_share']*monthly_profiles['loan_count']
        monthly_profiles = monthly_profiles[(monthly_profiles['date'].dt.year >= self.fc_start_date.year - 2) & (monthly_profiles['date'] <= self.fc_start_date.replace(day = 1))]
        
        monthly_profiles_agg = monthly_profiles.groupby(['day_id', 'weekday_id', 'country_code', 'loan_type']).sum().reset_index()
        monthly_profiles_agg['average_share'] = (monthly_profiles_agg['weighted_monthly_share_i']/monthly_profiles_agg['loan_count']).fillna(0)
        monthly_profiles_agg = monthly_profiles_agg.drop(['loan_count', 'loan_count_month_tot', 'month_id', 'monthly_share', 'weighted_monthly_share_i'], axis = 1)
        
        template = self.origination_output_template[['date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type', 'day_id', 'weekday_id']].drop_duplicates()
        
        self.intra_month_shares = template.merge(
            monthly_profiles_agg,
            how = 'left',
            on = ['day_id', 'weekday_id', 'country_code', 'loan_type']
            ) 
        self.intra_month_shares['average_share'] = self.intra_month_shares['average_share'].fillna(0)
        self.intra_month_shares = self.intra_month_shares.merge(
           self.intra_month_shares.groupby(['loan_cohort', 'country_code', 'currency_code', 'loan_type']).sum().reset_index(),
           how = 'left',
           on = ['loan_cohort', 'country_code', 'currency_code', 'loan_type'],
           suffixes = ('', '_month_tot')
           )
        self.intra_month_shares['average_share'] /= self.intra_month_shares['average_share_month_tot']
        self.intra_month_shares = self.intra_month_shares.drop(['average_share_month_tot'], axis = 1)
        share_of_start_month = ((self.fc_start_date.replace(month = self.fc_start_date.month % 12 + 1, day = 1) - pd.DateOffset(days = 1)).day - self.fc_start_date.day)/(self.fc_start_date.replace(month = self.fc_start_date.month % 12 + 1, day = 1) - pd.DateOffset(days = 1)).day     
        self.intra_month_shares.loc[self.intra_month_shares['loan_cohort'] == self.fc_start_date.replace(day = 1), 'average_share'] *= share_of_start_month
        
    def generate_daily_origination_forecast(self):
        
        print('Splitting monthly origination to daily forecast...')
        
        self.daily_origination_forecast = self.intra_month_shares.merge(
            self.monthly_origination_forecast,
            how = 'left',
            on = ['loan_cohort', 'country_code', 'currency_code', 'loan_type']
            )
        self.daily_origination_forecast['principal'] *= self.daily_origination_forecast['average_share']
        self.daily_origination_forecast['loan_count'] *= self.daily_origination_forecast['average_share']
        self.daily_origination_forecast = self.daily_origination_forecast.drop(['average_share'], axis = 1)
        
        self.daily_origination_forecast = self.daily_origination_forecast.merge(
            self.customer_split_fc,
            how = 'left',
            on = ['date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type']
            )
        
        self.daily_origination_forecast = pd.concat([
            
            self.daily_origination_forecast.assign(
                
                loan_count = self.daily_origination_forecast['loan_count'] * self.daily_origination_forecast['new_customer_share'],
                principal = self.daily_origination_forecast['principal'] * self.daily_origination_forecast['new_customer_share'],
                origination_type = 'new_customer'
                
                ),
            
            self.daily_origination_forecast.assign(
                
                loan_count = self.daily_origination_forecast['loan_count'] * (1- self.daily_origination_forecast['new_customer_share']),
                principal = self.daily_origination_forecast['principal'] * (1-self.daily_origination_forecast['new_customer_share']),
                origination_type = 'existing_customer'
                
                )
            
            ]).drop(['new_customer_share'], axis = 1)
        
        daily_origination_forecast_new_customers = self.daily_origination_forecast[self.daily_origination_forecast['origination_type'] == 'new_customer'].merge(
            self.creditor_split_fc,
            how = 'left',
            on = ['date', 'country_code']
            )
        daily_origination_forecast_new_customers['principal'] *= daily_origination_forecast_new_customers['creditor_share_new_customers']
        daily_origination_forecast_new_customers['loan_count'] *= daily_origination_forecast_new_customers['creditor_share_new_customers']
        daily_origination_forecast_new_customers = daily_origination_forecast_new_customers.drop(['creditor_share_new_customers'], axis = 1)
        
        self.daily_origination_forecast = pd.concat([
            
            self.daily_origination_forecast[self.daily_origination_forecast['origination_type'] == 'existing_customer'],
            daily_origination_forecast_new_customers
            
            ])

        self.daily_origination_forecast.loc[self.daily_origination_forecast['origination_type'] == 'existing_customer','creditor_id'] = 'creditor_mix_of_current_portfolio'
        
    def generate_future_origination_months(self, months_up_until_last_month):
        
        if months_up_until_last_month['loan_cohort'].max() == self.fc_end_date.replace(day = 1):
            
            return months_up_until_last_month
        
        else:
            
            new_month_data = months_up_until_last_month[months_up_until_last_month['loan_cohort'] == months_up_until_last_month['loan_cohort'].max()]            
            new_month_data['loan_cohort'] = new_month_data['loan_cohort'] + pd.DateOffset(months = 1)
            
            new_month_data = new_month_data.merge(
                self.origination_monthly_growth_rates[['loan_cohort', 'country_code', 'currency_code', 'loan_type', 'balance_growth', 'loan_growth']],
                how = 'left',
                on = ['loan_cohort', 'country_code', 'currency_code', 'loan_type']
                )
            new_month_data['principal_per_loan'] *= new_month_data['balance_growth']
            new_month_data['loan_count'] *= new_month_data['loan_growth']
            new_month_data['principal'] = new_month_data['principal_per_loan']*new_month_data['loan_count']
            new_month_data = new_month_data.drop(['balance_growth', 'loan_growth'], axis = 1)
            new_month_data['data_source'] = 'forecast'
            
            return pd.concat([
                
                self.generate_future_origination_months(new_month_data),
                months_up_until_last_month
                
                ])
        
    def generate_current_cohort_frontbook_forecast(self):
        
        if self.fc_start_date == self.fc_first_full_month_start_date:
            
            self.current_cohort_frontbook_forecast = pd.DataFrame()
            
        else:
            
            origination_fc = self.daily_origination_forecast[['date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type', 'creditor_id', 'principal', 'loan_count']].groupby([
                'date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type', 'creditor_id'
                ]).sum().reset_index()
            origination_fc['principal_per_loan'] = origination_fc['principal'] / origination_fc['loan_count']
            
            starting_point = self.raw_balance_data[
                (self.raw_balance_data['date']<self.fc_start_date) &
                (self.raw_balance_data['loan_cohort'] == dt.datetime(self.fc_start_date.year,self.fc_start_date.month,1))
                ]
            starting_point = starting_point[['date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type', 'creditor_id', 'principal_balance', 'loan_count']].groupby([
                'date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type', 'creditor_id'
                ]).sum().reset_index()
            starting_point = starting_point[starting_point['date'] == starting_point['date'].max()]
            
            intra_month_origination = origination_fc[(origination_fc['date'].dt.year == self.fc_start_date.year) & (origination_fc['date'].dt.month == self.fc_start_date.month)]
            intra_month_origination['principal_balance'] = intra_month_origination['principal_per_loan'] * intra_month_origination['loan_count']
            intra_month_origination = intra_month_origination.drop(['principal_per_loan', 'principal'], axis = 1)
            
            current_creditor_mix = self.generate_creditor_mix_of_latest_balance(self.raw_balance_data)
    
            new_customer_origination = intra_month_origination[intra_month_origination['creditor_id'] != 'creditor_mix_of_current_portfolio']

            existing_customer_origination = intra_month_origination[intra_month_origination['creditor_id'] == 'creditor_mix_of_current_portfolio'].merge(
                current_creditor_mix,
                how = 'left',
                on = ['country_code'],
                suffixes = ('_old', '')
                )
            existing_customer_origination['loan_count'] *= existing_customer_origination['creditor_share']
            existing_customer_origination['principal_balance'] *= existing_customer_origination['creditor_share']
            existing_customer_origination = existing_customer_origination.drop(['creditor_share', 'creditor_id_old'], axis = 1)
    
            intra_month_origination = pd.concat([            
                existing_customer_origination,
                new_customer_origination
                ])

            self.daily_origination_forecast = pd.concat([
                    
                self.generate_date_and_age_attributes(
                    intra_month_origination.rename(columns = {'principal_balance': 'principal'}).assign(data_source = 'forecast'),
                    'date',
                    'loan_cohort'
                    ),
                
                self.daily_origination_forecast[self.daily_origination_forecast['date'] >= (self.fc_start_date + pd.DateOffset(months = 1)).to_period('M').to_timestamp()]
                
                ])
            
            full_month_fc = pd.concat([
                starting_point,
                intra_month_origination
                ])
            
            country_currency_pairs = full_month_fc[['country_code', 'currency_code']].drop_duplicates()
            country_creditor_pairs = full_month_fc[['country_code', 'creditor_id']].drop_duplicates()
            
            dates = full_month_fc['date'].drop_duplicates().to_list()
            cohorts = full_month_fc['loan_cohort'].drop_duplicates().to_list()
            loan_types = full_month_fc['loan_type'].drop_duplicates().to_list()
            countries = full_month_fc['country_code'].drop_duplicates().to_list()
    
            full_month_fc = pd.DataFrame(
                list(itertools.product(dates, cohorts, countries, loan_types)),
                columns = ['date', 'loan_cohort', 'country_code', 'loan_type']).drop_duplicates().merge(
                    country_currency_pairs,
                    how = 'left',
                    on = ['country_code']
                    ).merge(
                        country_creditor_pairs,
                        how = 'left',
                        on = ['country_code']
                        ).merge(
                            full_month_fc,
                            how = 'left',
                            on = ['date', 'loan_cohort', 'country_code','currency_code', 'loan_type', 'creditor_id']
                            )
            full_month_fc['principal_balance'] = full_month_fc['principal_balance'].fillna(0)
            full_month_fc['loan_count'] = full_month_fc['loan_count'].fillna(0)
            
            full_month_fc = full_month_fc.groupby(['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code', 'creditor_id']).sum().sort_values(by='date').groupby([
                'loan_cohort', 'loan_type', 'country_code', 'currency_code', 'creditor_id'
                ]).cumsum().reset_index()
            
            full_month_fc['principal_balance'] = full_month_fc.groupby([
                'loan_cohort', 'loan_type', 'country_code', 'currency_code', 'creditor_id'
                ])['principal_balance'].transform(lambda v: v.ffill())
            full_month_fc = full_month_fc[full_month_fc['date'] >= self.fc_start_date]
            
            start_day_in_month = full_month_fc['date'].min().day
            
            adjusted_balance_to_loan_ratios = self.balance_to_origination_ratios.merge(
                self.balance_to_origination_ratios[self.balance_to_origination_ratios['day_id'] == start_day_in_month].drop(['day_id'], axis = 1),
                how = 'left',
                on = ['loan_type', 'country_code', 'currency_code'],
                suffixes = ('', '_base_value')
                )
            
            adjusted_balance_to_loan_ratios['cum_balance_ratio'] /= adjusted_balance_to_loan_ratios['cum_balance_ratio_base_value']
            adjusted_balance_to_loan_ratios['cum_loan_ratio'] /= adjusted_balance_to_loan_ratios['cum_loan_ratio_base_value']
            adjusted_balance_to_loan_ratios = adjusted_balance_to_loan_ratios.drop(['cum_loan_ratio_base_value', 'cum_balance_ratio_base_value'], axis = 1)
            adjusted_balance_to_loan_ratios = adjusted_balance_to_loan_ratios[adjusted_balance_to_loan_ratios['day_id'] >= self.fc_start_date.day]        
            adjusted_balance_to_loan_ratios['cum_balance_ratio'] = adjusted_balance_to_loan_ratios['cum_balance_ratio'].clip(0,1)
            adjusted_balance_to_loan_ratios['cum_loan_ratio'] = adjusted_balance_to_loan_ratios['cum_loan_ratio'].clip(0,1)
            
            self.current_cohort_frontbook_forecast = full_month_fc.assign(day_id = full_month_fc['date'].dt.day).merge(
                adjusted_balance_to_loan_ratios,
                how = 'left',
                on = ['loan_type', 'country_code', 'currency_code', 'day_id']
                ).assign(data_source = 'front_book_current_cohort')
            
            self.current_cohort_frontbook_forecast['principal_balance'] *= self.current_cohort_frontbook_forecast['cum_balance_ratio']
            self.current_cohort_frontbook_forecast['loan_count'] *= self.current_cohort_frontbook_forecast['cum_loan_ratio']        
            self.current_cohort_frontbook_forecast['balance_per_loan'] = self.current_cohort_frontbook_forecast['principal_balance']/self.current_cohort_frontbook_forecast['loan_count']
            self.current_cohort_frontbook_forecast = self.current_cohort_frontbook_forecast.drop(['cum_balance_ratio', 'cum_loan_ratio'], axis = 1)
            self.current_cohort_frontbook_forecast = self.generate_date_and_age_attributes(
                self.current_cohort_frontbook_forecast,
                'date',
                'loan_cohort'
                )
                        
            print('Generating cohort: ' + self.current_cohort_frontbook_forecast['loan_cohort'].max().strftime('%Y-%m') + '...')
    
            self.current_cohort_frontbook_forecast = self.generate_new_days(self.current_cohort_frontbook_forecast, 'front_book_current_cohort')
        
    def generate_future_cohorts_frontbook_forecast(self):
        
        self.frontbook_forecast = pd.DataFrame()
        
        if self.fc_first_full_month_start_date <= self.fc_end_date:
        
            origination_fc = self.daily_origination_forecast[['date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type', 'creditor_id', 'principal', 'loan_count']].groupby([
                'date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type', 'creditor_id'
                ]).sum().reset_index()
    
            cumulative_origination_fc = origination_fc[origination_fc['date'] >= self.fc_first_full_month_start_date]
            cumulative_origination_fc = cumulative_origination_fc.groupby([
                'date', 'loan_cohort', 'loan_type', 'creditor_id', 'country_code', 'currency_code'
                ]).sum().sort_values(by='date').groupby(['loan_cohort', 'loan_type', 'creditor_id', 'country_code', 'currency_code']).cumsum().reset_index()
            
            first_month_cohort_fc = self.balance_output_template[(self.balance_output_template['age_bucket'] == '0M') & (self.balance_output_template['date'] >= self.fc_first_full_month_start_date)].assign(day_id = self.balance_output_template['date'].dt.day).merge(
                cumulative_origination_fc,
                how = 'left',
                on = ['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code']
                )
            first_month_cohort_fc['principal'] = first_month_cohort_fc.groupby(['loan_cohort', 'loan_type', 'creditor_id', 'country_code', 'currency_code'])['principal'].transform(lambda v: v.ffill())        
            first_month_cohort_fc = first_month_cohort_fc.merge(
                self.balance_to_origination_ratios,
                how = 'left',
                on = ['loan_type', 'country_code', 'currency_code', 'day_id']
                )
            first_month_cohort_fc['principal'] *= first_month_cohort_fc['cum_balance_ratio'].fillna(1)
            first_month_cohort_fc['loan_count'] *= first_month_cohort_fc['cum_loan_ratio'].fillna(1)
            first_month_cohort_fc = first_month_cohort_fc.drop(['cum_balance_ratio', 'cum_loan_ratio'], axis = 1)
            first_month_cohort_fc = first_month_cohort_fc.rename(columns = {'principal': 'principal_balance'})
            first_month_cohort_fc['balance_per_loan'] = first_month_cohort_fc['principal_balance'] / first_month_cohort_fc['loan_count']
            first_month_cohort_fc = self.generate_date_and_age_attributes(first_month_cohort_fc, 'date', 'loan_cohort')
            first_month_cohort_fc['data_source'] = 'front_book_future_cohorts'
            
            latest_balance = pd.concat([
                self.backbook_forecast,
                self.current_cohort_frontbook_forecast
                ])
            creditor_mix = self.generate_creditor_mix_of_latest_balance(latest_balance[latest_balance['date'] == self.fc_first_full_month_start_date])
            
            for cohorts in first_month_cohort_fc['loan_cohort'].drop_duplicates().to_list():
                
                print('Generating cohort: ' + cohorts.strftime('%Y-%m') + '...')
                
                first_month_cohort_fc_i = first_month_cohort_fc[first_month_cohort_fc['loan_cohort'] == cohorts]
                first_month_cohort_fc_i_new_customers = first_month_cohort_fc_i[first_month_cohort_fc_i['creditor_id'] != 'creditor_mix_of_current_portfolio']
                first_month_cohort_fc_i_existing_customers = first_month_cohort_fc_i[first_month_cohort_fc_i['creditor_id'] == 'creditor_mix_of_current_portfolio'].merge(
                    creditor_mix,
                    how = 'left',
                    on = ['country_code'],
                    suffixes = ('_old', '' )
                    )
                first_month_cohort_fc_i_existing_customers['loan_count'] *= first_month_cohort_fc_i_existing_customers['creditor_share']
                first_month_cohort_fc_i_existing_customers['principal_balance'] *= first_month_cohort_fc_i_existing_customers['creditor_share']
                first_month_cohort_fc_i_existing_customers['balance_per_loan'] = first_month_cohort_fc_i_existing_customers['principal_balance']/first_month_cohort_fc_i_existing_customers['loan_count']
                first_month_cohort_fc_i_existing_customers = first_month_cohort_fc_i_existing_customers.drop(['creditor_share', 'creditor_id_old'], axis = 1)
                
                first_month_cohort_fc_i = pd.concat([
                    first_month_cohort_fc_i_existing_customers,
                    first_month_cohort_fc_i_new_customers
                    ])
                
                self.frontbook_forecast = pd.concat([
                    self.frontbook_forecast,
                    self.generate_new_days(first_month_cohort_fc_i[first_month_cohort_fc_i['loan_cohort'] == cohorts], 'front_book_future_cohorts')
                    ])
                
                latest_balance = pd.concat([
                    self.backbook_forecast,
                    self.current_cohort_frontbook_forecast,
                    self.frontbook_forecast
                    ])
                
                cohort_i_origination = self.daily_origination_forecast[
                    (self.daily_origination_forecast['loan_cohort'] == cohorts) &
                    (self.daily_origination_forecast['origination_type'] == 'existing_customer')
                    ].drop(['creditor_id'], axis = 1).merge(
                        creditor_mix,
                        how = 'left',
                        on = ['country_code']
                        )                    
                cohort_i_origination['principal'] *= cohort_i_origination['creditor_share']
                cohort_i_origination['loan_count'] *= cohort_i_origination['creditor_share']
                cohort_i_origination = cohort_i_origination.drop(['creditor_share'], axis = 1)
                
                self.daily_origination_forecast = pd.concat([
                    
                    self.daily_origination_forecast[self.daily_origination_forecast['loan_cohort'] != cohorts],
                    
                    self.daily_origination_forecast[
                        (self.daily_origination_forecast['loan_cohort'] == cohorts) &
                        (self.daily_origination_forecast['origination_type'] == 'new_customer')   
                        ],
                    
                    cohort_i_origination
                    
                    ])
                
                creditor_mix = self.generate_creditor_mix_of_latest_balance(latest_balance[latest_balance['date'] == cohorts])
            
    def generate_final_output(self):
        
        self.main_balance_output = self.main_balance_output = self.generate_date_and_age_attributes(
            
            pd.concat([
                self.current_cohort_frontbook_forecast.drop(['balance_per_loan'], axis = 1, errors = 'ignore'),
                self.frontbook_forecast.drop(['balance_per_loan'], axis = 1, errors = 'ignore'),
                self.backbook_forecast.drop(['balance_per_loan'], axis = 1),
                self.raw_balance_data[
                    (self.raw_balance_data['date'] < self.fc_start_date) &
                    (self.raw_balance_data['date'] >= self.fc_start_date + pd.DateOffset(years = -1))][
                        ['date', 'loan_cohort', 'country_code', 'loan_type', 'creditor_id', 'currency_code', 'principal_balance', 'loan_count']
                        ].groupby(
                            ['date', 'loan_cohort', 'country_code', 'loan_type', 'creditor_id', 'currency_code']
                            ).sum().reset_index().assign(data_source = 'actual')
                ]),
            'date',
            'loan_cohort'
            )
        
        self.main_balance_output = self.main_balance_output.assign(
            principal_balance_SEK = self.main_balance_output.principal_balance,
            principal_balance_EUR = self.main_balance_output.principal_balance,
            principal_balance_USD = self.main_balance_output.principal_balance
            )
        self.main_balance_output.loc[self.main_balance_output['currency_code'] == 'EUR', 'principal_balance_SEK'] *= self.SEK_EUR_plan_rate
        self.main_balance_output.loc[self.main_balance_output['currency_code'] == 'SEK', 'principal_balance_SEK'] *= 1.0
        self.main_balance_output.loc[self.main_balance_output['currency_code'] == 'EUR', 'principal_balance_EUR'] *= 1.0
        self.main_balance_output.loc[self.main_balance_output['currency_code'] == 'SEK', 'principal_balance_EUR'] /= self.SEK_EUR_plan_rate
        self.main_balance_output.loc[self.main_balance_output['currency_code'] == 'EUR', 'principal_balance_USD'] *= self.EUR_USD_plan_rate
        self.main_balance_output.loc[self.main_balance_output['currency_code'] == 'SEK', 'principal_balance_USD'] *= self.SEK_USD_plan_rate
        self.main_balance_output = self.main_balance_output.assign(forecast_version_id = self.forecast_version_id)
        self.main_balance_output = self.main_balance_output[self.main_balance_output['date'] <= self.fc_end_date_output]        
        self.main_balance_output.to_csv(self.data_storage_path + 'main_balance_output.csv', index = False)
        
        self.main_origination_output = self.generate_date_and_age_attributes(
            pd.concat([
                self.raw_origination_data[
                    (self.raw_origination_data['date'] < self.fc_start_date) &
                    (self.raw_origination_data['date'] >= self.fc_start_date + pd.DateOffset(years = -1))
                    ].assign(data_source = 'actual'),
                self.daily_origination_forecast
                ]),
            'date',
            'loan_cohort'
            ).assign(forecast_version_id = self.forecast_version_id)
        self.main_origination_output = self.main_origination_output[self.main_origination_output['date'] <= self.fc_end_date_output]        
        self.main_origination_output.to_csv(self.data_storage_path + 'main_origination_output.csv', index = False)

        self.credit_risk_indicator_forecast = self.credit_risk_indicator_forecast[self.credit_risk_indicator_forecast['date'] <= self.fc_end_date_output]        
        self.credit_risk_indicator_forecast.to_csv(self.data_storage_path + 'credit_risk_indicators_output.csv', index = False)
        
    def generate_creditor_mix_of_latest_balance(self, latest_balance_data):
        
        creditor_mix = latest_balance_data[latest_balance_data['date'] == latest_balance_data['date'].max()]
        creditor_mix = creditor_mix[['country_code', 'creditor_id', 'loan_count']].groupby(['country_code', 'creditor_id']).sum().reset_index()
        
        creditor_mix = creditor_mix.merge(
            creditor_mix.groupby(['country_code']).sum().reset_index(),
            how = 'left',
            on = ['country_code'],
            suffixes = ('', '_country_tot')
            )
        creditor_mix['creditor_share'] = creditor_mix['loan_count']/creditor_mix['loan_count_country_tot']        
        creditor_mix = creditor_mix.drop(['loan_count', 'loan_count_country_tot'], axis = 1)
        
        return creditor_mix
    
    def generate_baseline_fc(self):
        
        # Model setup
        
        self.load_raw_data()
        self.generate_additional_datasets()
        self.generate_monthly_origination_growth_rates()
        self.generate_new_vs_old_customer_split()
        self.estimate_balance_growth_factors()
        self.generate_first_month_balance_to_origination_ratios()
        self.estimate_intra_month_origination_profile()
        self.estimate_credit_indicator_shares()
        self.generate_creditor_distribution_new_customers()

        # Origination forecast

        self.generate_monthly_origination_forecast()
        self.generate_daily_origination_forecast()

        # Balance forecast

        self.generate_backbook_forecast()
        self.generate_current_cohort_frontbook_forecast()
        self.generate_future_cohorts_frontbook_forecast()
        
        # Additional metrics forecast
        
        self.generate_credit_risk_indicator_forecast()
        self.generate_daily_cashflows()
        
        # Export output
        
        self.generate_final_output()
        
    def estimate_credit_indicator_shares(self):
        
        eligibility_type_list = credit_indicator_shares = self.raw_credit_risk_indicator_data['eligibility_type'].drop_duplicates().to_list()
        cpd_type_list = self.raw_credit_risk_indicator_data['cpd'].drop_duplicates().to_list()
        credit_score_list = self.raw_credit_risk_indicator_data['score_bucket'].drop_duplicates().to_list()
        holiday_list = self.raw_credit_risk_indicator_data['on_holiday'].drop_duplicates().to_list()
        age_list = self.raw_credit_risk_indicator_data['age_group'].drop_duplicates().to_list()
        day_id_list = range(1,32)
        country_list = self.raw_credit_risk_indicator_data['country_code'].drop_duplicates().to_list()
        country_currency_pairs = self.raw_credit_risk_indicator_data[['country_code', 'currency_code']].drop_duplicates()
        
        template = pd.DataFrame(
            list(itertools.product(country_list, eligibility_type_list, cpd_type_list, credit_score_list, holiday_list, age_list, day_id_list)),
            columns = ['country_code', 'eligibility_type', 'cpd', 'score_bucket', 'on_holiday', 'age_group', 'day_id']).drop_duplicates().merge(
                country_currency_pairs,
                how = 'left',
                on = ['country_code']
                )
                
        self.credit_risk_indicator_shares = template.merge(
            self.raw_credit_risk_indicator_data,
            how = 'left',
            on = ['country_code', 'currency_code', 'eligibility_type', 'cpd', 'score_bucket', 'on_holiday', 'age_group', 'day_id']
            ).fillna(0)
        
        self.credit_risk_indicator_shares = self.credit_risk_indicator_shares.merge(
            self.credit_risk_indicator_shares[['country_code', 'currency_code', 'age_group', 'day_id', 'principal_balance', 'loan_count']].groupby(['country_code', 'currency_code', 'age_group', 'day_id']).sum().reset_index(),
            how = 'left',
            on = ['country_code', 'currency_code', 'age_group', 'day_id'],
            suffixes = ('', '_tot')
            )
        
        self.credit_risk_indicator_shares['principal_share'] = (self.credit_risk_indicator_shares['principal_balance'] / self.credit_risk_indicator_shares['principal_balance_tot']).replace([-np.inf, np.inf], np.nan).fillna(0)
        self.credit_risk_indicator_shares['loan_share'] = (self.credit_risk_indicator_shares['loan_count'] / self.credit_risk_indicator_shares['loan_count_tot']).replace([-np.inf, np.inf], np.nan).fillna(0)
        self.credit_risk_indicator_shares = self.credit_risk_indicator_shares.drop(['principal_balance', 'principal_balance_tot', 'loan_count', 'loan_count_tot'], axis = 1)
        
    def generate_credit_risk_indicator_forecast(self):
        
        self.credit_risk_indicator_forecast = pd.concat([
                        self.current_cohort_frontbook_forecast.drop(['balance_per_loan'], axis = 1, errors = 'ignore'),
                        self.frontbook_forecast.drop(['balance_per_loan'], axis = 1, errors = 'ignore'),
                        self.backbook_forecast.drop(['balance_per_loan'], axis = 1)
                        
                        ])
                
        self.credit_risk_indicator_forecast['cohort_age_months'] = 12 * (self.credit_risk_indicator_forecast['date'].dt.year - self.credit_risk_indicator_forecast['loan_cohort'].dt.year) + (self.credit_risk_indicator_forecast['date'].dt.month - self.credit_risk_indicator_forecast['loan_cohort'].dt.month)
        bins = [-np.inf, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, np.inf]
        labels = ['0M', '1M', '2M', '3M', '4M', '5M', '6M', '7M', '8M', '9M', '10M', '11M', '12M', '>12M']
        self.credit_risk_indicator_forecast['age_group'] = pd.cut(self.credit_risk_indicator_forecast['cohort_age_months'], bins = bins, labels = labels)        
        self.credit_risk_indicator_forecast['day_id'] = self.credit_risk_indicator_forecast['date'].dt.day        
        self.credit_risk_indicator_forecast = self.credit_risk_indicator_forecast.merge(
            self.credit_risk_indicator_shares,
            how = 'left',
            on = ['country_code', 'currency_code', 'age_group', 'day_id']
            )
        self.credit_risk_indicator_forecast['principal_balance'] *= self.credit_risk_indicator_forecast['principal_share']
        self.credit_risk_indicator_forecast['loan_count'] *= self.credit_risk_indicator_forecast['loan_share']
        
        self.credit_risk_indicator_forecast = self.credit_risk_indicator_forecast[['date', 'country_code', 'currency_code', 'creditor_id', 'eligibility_type', 'cpd', 'on_holiday', 'score_bucket', 'principal_balance', 'loan_count']].groupby(
            ['date', 'country_code', 'currency_code', 'creditor_id', 'eligibility_type', 'cpd', 'on_holiday', 'score_bucket']
            ).sum().reset_index()
        
    def generate_daily_cashflows(self):
        
        balance_output = pd.concat([
            
            self.current_cohort_frontbook_forecast.drop(['balance_per_loan'], axis = 1, errors = 'ignore'),
            self.frontbook_forecast.drop(['balance_per_loan'], axis = 1, errors = 'ignore'),
            self.backbook_forecast.drop(['balance_per_loan'], axis = 1),
            self.raw_balance_data[
                (self.raw_balance_data['date'] < self.fc_start_date) &
                (self.raw_balance_data['date'] >= self.fc_start_date + pd.DateOffset(years = -1))][
                    ['date', 'loan_cohort', 'country_code', 'loan_type', 'creditor_id', 'currency_code', 'principal_balance', 'loan_count']
                    ].groupby(
                        ['date', 'loan_cohort', 'country_code', 'loan_type', 'creditor_id', 'currency_code']
                        ).sum().reset_index().assign(data_source = 'actual')
            ])[['date', 'country_code', 'currency_code', 'creditor_id', 'principal_balance']].groupby(['date', 'country_code', 'currency_code', 'creditor_id']).sum().reset_index()
                        
        origination_output = pd.concat([
                
                self.raw_origination_data[
                        (self.raw_origination_data['date'] < self.fc_start_date) &
                        (self.raw_origination_data['date'] >= self.fc_start_date + pd.DateOffset(years = -1))
                        ].assign(data_source = 'actual'),
                    self.daily_origination_forecast
                    ])[['date', 'country_code', 'currency_code', 'creditor_id', 'principal']].groupby(['date', 'country_code', 'currency_code', 'creditor_id']).sum().reset_index()
        
        date_list = balance_output['date'].drop_duplicates().to_list()
        country_list = balance_output['country_code'].drop_duplicates().to_list()
        creditor_list = list(set(balance_output['creditor_id'].drop_duplicates().to_list() +  origination_output['creditor_id'].drop_duplicates().to_list()))
        country_currency_pairs = balance_output[['country_code', 'currency_code']].drop_duplicates()
        
        template = pd.DataFrame(
            list(itertools.product(date_list, country_list, creditor_list)),
            columns = ['date', 'country_code', 'creditor_id']).drop_duplicates().merge(
                country_currency_pairs,
                how = 'left',
                on = ['country_code']
                )
                
        self.daily_cashflows = template.merge(
            balance_output,
            how = 'left',
            on = ['date', 'country_code', 'currency_code', 'creditor_id']
            ).merge(
                origination_output.rename(columns = {'principal':'origination'}),
                how = 'left',
                on = ['date', 'country_code', 'currency_code', 'creditor_id']
                )
                
        self.daily_cashflows = self.daily_cashflows.merge(
            self.daily_cashflows.assign(date = self.daily_cashflows['date'] + pd.DateOffset(days = 1)),
            how = 'left',
            on = ['date', 'country_code', 'currency_code', 'creditor_id'],
            suffixes = ('', '_yesterday')
            )
        
        self.daily_cashflows['repayments'] = self.daily_cashflows['principal_balance'].fillna(0) - self.daily_cashflows['principal_balance_yesterday'].fillna(0) - self.daily_cashflows['origination'].fillna(0)
        self.daily_cashflows = self.daily_cashflows.drop(['principal_balance_yesterday', 'origination_yesterday'], axis = 1)
        self.daily_cashflows['repayments']*=-1
        self.daily_cashflows['origination']*=-1
        self.daily_cashflows.to_csv(self.data_storage_path + 'portfolio_daily_cash_flows.csv', index = False)
        
        