from AnyfinSPVModel import *
import pandas as pd
import itertools
import numpy as np
import datetime as dt
import warnings
from scipy.optimize import curve_fit
import os
import holidays
from google.cloud import storage

warnings.filterwarnings('ignore')


class AnyfinPortfolioForecast:

    def __init__(self, settings):

        self.settings = settings
        self.fc_start_date = settings.fc_start_date
        self.fc_end_date_output = settings.fc_end_date
        self.SEK_USD_plan_rate = settings.SEK_USD_plan_rate
        self.SEK_EUR_plan_rate = settings.SEK_EUR_plan_rate
        self.EUR_USD_plan_rate = settings.EUR_USD_plan_rate
        self.included_countries = settings.included_countries
        self.load_new_data = settings.load_new_data
        self.creditor_split_new_customers = settings.creditor_split_new_customers
        self.adjustments = settings.adjustments
        self.ignored_periods = settings.ignored_periods
        self.new_markets = settings.new_markets
        self.funding_costs_assumptions = settings.funding_costs_assumptions
        self.revenue_assumptions = settings.revenue_assumptions
        self.spv_settings = settings.spv_settings
        self.spv_lenders = settings.spv_lenders
        self.spv_draw_downs = settings.spv_draw_downs

        self.forecast_version_id = str(dt.datetime.now()) + ' ' + os.getlogin()

        self.data_storage_path = os.getcwd() + os.sep + 'Data' + os.sep
        if os.path.exists(self.data_storage_path) == False:
            os.makedirs(self.data_storage_path)

        #        self.data_storage_path = 'gs://anyfin-treasure-forecast/'

        if self.fc_start_date.day == 1:

            self.fc_first_full_month_start_date = self.fc_start_date

        else:

            self.fc_first_full_month_start_date = dt.datetime(
                (self.fc_start_date + pd.DateOffset(months=1)).year,
                (self.fc_start_date + pd.DateOffset(months=1)).month,
                1
            )

        self.fc_end_date = self.fc_end_date_output.replace(day=self.fc_end_date_output.days_in_month)

    def load_raw_data(self):

        ###########################################################################################################################################################################
        #
        # About load_raw_data()
        #
        # Loads all input data needed to train the model as well as display actual data along witht the forecast output. If load_new_data = True, data will be loaded
        # from BigQuery, otherwise it will be loaded from a local csv-file
        # 
        ###########################################################################################################################################################################

        country_string = ''

        for countries in self.included_countries:
            if countries != self.included_countries[len(self.included_countries) - 1]:
                country_string = country_string + """'""" + countries + """', """
            else:
                country_string = country_string + """'""" + countries + """' """

        if self.load_new_data == True:

            print('Loading data from BigQuery...')

            self.raw_balance_data = pd.read_gbq(

                """


                WITH

########################################################################################                
#
# This section is removed until expected end data is in place
#                             
#                    expected_end_dates AS (
#
#                      SELECT
#                        month,
#                        loan_id,
#                        NULL AS expected_end_date,
#                        NULL AS original_months,
#                        loan_cohort
#                      FROM anyfin.finance.monthly_loan_balances_gross
#                    
#                      ),
#########################################################################################


                    loan_first_due_dates AS (

                      SELECT
                        loan_id,
                        MIN(due_date) AS first_due_date
                      FROM anyfin.finance.cycle_balances c
                      GROUP BY 1

                  )

                    SELECT
                      dlb.date,
                      c.name AS creditor_id,
                      IF(l.type='annuity','annuity', 'flex') AS loan_type,
                      dlb.country_code,
                      dlb.currency_code,
                      DATE_TRUNC(dlb.loan_cohort, MONTH) AS loan_cohort,
                      dlb.months_since,

                      CASE
                        WHEN dlb.country_code = 'DE' AND DATE_TRUNC(DATE_ADD(dlb.loan_cohort, INTERVAL 1 MONTH), MONTH) = DATE_TRUNC(lfd.first_due_date, MONTH) THEN 'short'
                        WHEN dlb.country_code = 'DE' THEN 'long'
                        WHEN DATE_TRUNC(lfd.first_due_date, MONTH) = DATE_TRUNC(dlb.loan_cohort, MONTH) THEN 'short'
                        ELSE 'long'
                      END AS first_cycle_type,

                      CASE
                          WHEN dlb.currency_code = 'SEK' AND dlb.principal_balance_excl_defaults > 100 THEN TRUE
                          WHEN dlb.currency_code = 'EUR' AND dlb.principal_balance_excl_defaults > 10 THEN TRUE
                          ELSE FALSE
                      END AS amount_significant,
                      SUM(dlb.principal_balance) AS principal_balance,
                      SUM(dlb.principal_balance_excl_defaults) AS principal_balance_excl_defaults,
                      SUM(dlb.revenue) AS revenue,
                      IFNULL(
                          SAFE_DIVIDE(
                            SUM((CASE WHEN dlb.country_code = 'FI' THEN effective_apr ELSE dlb.interest_rate END) * dlb.principal_balance),
                            SUM(dlb.principal_balance)
                          )/100
                        ,0
                        ) AS avg_interest,
                      COUNT(dlb.loan_id) AS loan_count,                     
#                      SAFE_DIVIDE(
#                          SUM(DATE_DIFF(eed.expected_end_date, dlb.date, MONTH) * dlb.principal_balance),
#                          SUM(dlb.principal_balance)
#                      ) AS avg_remaining_term,
#                      
#                      SAFE_DIVIDE(
#                          SUM(eed.original_months * dlb.principal_balance),
#                          SUM(dlb.principal_balance)
#                      ) AS avg_original_term

                    FROM anyfin.finance.daily_loan_balances dlb
                    LEFT JOIN main.loans l ON l.id = dlb.loan_id
                    LEFT JOIN main.creditors c ON c.id = dlb.creditor_id
                    LEFT JOIN loan_first_due_dates lfd ON lfd.loan_id = dlb.loan_id
 #                   LEFT JOIN expected_end_dates eed ON eed.loan_id = dlb.loan_id AND DATE_TRUNC(eed.month, MONTH) = DATE_TRUNC(dlb.date, MONTH)
                    WHERE dlb.date < '""" + self.fc_start_date.strftime(
                    '%Y-%m-%d') + """' AND dlb.principal_balance > 0 AND dlb.country_code IN (""" + country_string + """)
                    GROUP BY 1,2,3,4,5,6,7,8,9

                """,

                project_id='anyfin'

            )

            self.raw_balance_data['date'] = pd.to_datetime(self.raw_balance_data['date'], format='%Y-%m-%d')
            self.raw_balance_data['loan_cohort'] = pd.to_datetime(self.raw_balance_data['loan_cohort'],
                                                                  format='%Y-%m-%d')
            self.raw_balance_data.to_csv(self.data_storage_path + 'raw_balance_data.csv', index=False)

            self.raw_origination_data = pd.read_gbq(

                """

                WITH

                    loan_first_due_dates AS (

                      SELECT
                        loan_id,
                        MIN(due_date) AS first_due_date
                      FROM anyfin.finance.cycle_balances c
                      GROUP BY 1

                  )

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

                      CASE
                        WHEN dlb.country_code = 'DE' AND DATE_TRUNC(DATE_ADD(dlb.loan_cohort, INTERVAL 1 MONTH), MONTH) = DATE_TRUNC(lfd.first_due_date, MONTH) THEN 'short'
                        WHEN dlb.country_code = 'DE' THEN 'long'
                        WHEN DATE_TRUNC(lfd.first_due_date, MONTH) = DATE_TRUNC(dlb.loan_cohort, MONTH) THEN 'short'
                        ELSE 'long'
                      END AS first_cycle_type,

                      SUM(dlb.principal_balance) AS principal,
                      COUNT(dlb.loan_id) AS loan_count
                    FROM anyfin.finance.daily_loan_balances dlb
                    LEFT JOIN main.loans l ON l.id = dlb.loan_id
                    LEFT JOIN main.creditors c ON c.id = dlb.creditor_id
                    LEFT JOIN loan_first_due_dates lfd ON lfd.loan_id = dlb.loan_id
                    WHERE dlb.date = dlb.loan_cohort AND dlb.date < '""" + self.fc_start_date.strftime(
                    '%Y-%m-%d') + """' AND dlb.country_code IN (""" + country_string + """)
                    GROUP BY 1,2,3,4,5,6,7,8

                """,

                project_id='anyfin'

            )
            self.raw_origination_data['date'] = pd.to_datetime(self.raw_origination_data['date'], format='%Y-%m-%d')
            self.raw_origination_data['loan_cohort'] = pd.to_datetime(self.raw_origination_data['loan_cohort'],
                                                                      format='%Y-%m-%d')
            self.raw_origination_data.to_csv(self.data_storage_path + 'raw_origination_data.csv', index=False)

            filter_String = """"""
            for index, row in self.ignored_periods.iterrows():
                row['Country']

                filter_String = filter_String + """AND NOT (dlb.country_code = '""" + row[
                    'Country'] + """' AND dlb.date BETWEEN '""" + self.fc_start_date.strftime(
                    '%Y-%m-%d') + """' AND '""" + self.fc_end_date.strftime('%Y-%m-%d') + """') \n"""

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
                            WHEN dlb.cpd = 1 THEN '1 cpd'
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
                      LEFT JOIN finance.cycle_balances cyb ON cyb.loan_id = dlb.loan_id AND dlb.date BETWEEN cyb.start_date AND cyb.due_date AND dlb.date < '""" + self.fc_start_date.strftime(
                    '%Y-%m-%d') + """' AND dlb.country_code IN (""" + country_string + """)
                      WHERE dlb.principal_balance > 0 \n """ + filter_String + """
                      GROUP BY 1,2,3,4,5,6,7,8

                """,

                project_id='anyfin'

            )

            self.raw_credit_risk_indicator_data.to_csv(self.data_storage_path + 'raw_credit_risk_indicator_data.csv',
                                                       index=False)

            self.raw_credit_risk_indicator_data_recent = pd.read_gbq(

                """                
 
                     SELECT
                       dlb.date,
                       EXTRACT(DAY FROM dlb.date) AS day_id,
                       dlb.country_code,
                       dlb.currency_code,
                       cre.name AS creditor_id,
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
                           WHEN dlb.cpd = 1 THEN '1 cpd'
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
                     LEFT JOIN main.creditors cre ON cre.id = dlb.creditor_id
                     LEFT JOIN finance.loan_facts l on l.loan_id = dlb.loan_id
                     LEFT JOIN finance.loan_facts l2 on l2.source_loan_id = dlb.loan_id
                     LEFT JOIN finance.cycle_balances cyb ON cyb.loan_id = dlb.loan_id AND dlb.date BETWEEN cyb.start_date AND cyb.due_date AND dlb.country_code IN (""" + country_string + """)
                    WHERE dlb.principal_balance > 0 AND dlb.date >= '""" + (
                        self.fc_start_date + pd.DateOffset(days=-7)).strftime('%Y-%m-%d') + """'
                    GROUP BY 1,2,3,4,5,6,7,8,9,10

                """,

                project_id='anyfin'

            )
            self.raw_credit_risk_indicator_data_recent['date'] = pd.to_datetime(
                self.raw_credit_risk_indicator_data_recent['date'], format='%Y-%m-%d')
            self.raw_credit_risk_indicator_data_recent.to_csv(
                self.data_storage_path + 'raw_credit_risk_indicator_data_recent.csv', index=False)

            self.raw_customer_metric_data = pd.read_gbq(

                """

                    WITH

                      tot_debt_per_customer AS (

                        SELECT
                          date,
                          customer_id,
                          SUM(dlb.principal_balance) AS customer_tot_deb,
                        FROM anyfin.finance.daily_loan_balances dlb
                        GROUP BY 1,2

                      )

                    SELECT
                      dlb.date,
                      dlb.country_code,
                      dlb.currency_code,
                      CASE
                        WHEN (dlb.currency_code IN ('SEK', 'NOK') AND cd.customer_tot_deb > 100000) OR (dlb.currency_code = 'EUR' AND customer_tot_deb > 10000) THEN '>100KSEK'
                        ELSE '<100KSEK'
                      END AS debt_group,
                      SUM(dlb.principal_balance) AS principal_balance,  
                      COUNT(dlb.loan_id) AS loan_count,
                      COUNT(DISTINCT dlb.customer_id) AS customer_count,
                    FROM anyfin.finance.daily_loan_balances dlb
                    LEFT JOIN tot_debt_per_customer cd ON cd.customer_id = dlb.customer_id AND cd.date = dlb.date
                    WHERE dlb.principal_balance > 0 AND dlb.date < '""" + self.fc_start_date.strftime(
                    '%Y-%m-%d') + """' AND dlb.country_code IN (""" + country_string + """)
                    GROUP BY 1,2,3,4

                """,

                project_id='anyfin'

            )
            self.raw_customer_metric_data['date'] = pd.to_datetime(self.raw_customer_metric_data['date'],
                                                                   format='%Y-%m-%d')
            self.raw_customer_metric_data.to_csv(self.data_storage_path + 'raw_customer_metric_data.csv', index=False)

        else:

            print('Loading data from local csv-file...')

            self.raw_balance_data = pd.read_csv(self.data_storage_path + 'raw_balance_data.csv',
                                                parse_dates=['date', 'loan_cohort'])
            self.raw_origination_data = pd.read_csv(self.data_storage_path + 'raw_origination_data.csv',
                                                    parse_dates=['date', 'loan_cohort'])
            self.raw_credit_risk_indicator_data = pd.read_csv(
                self.data_storage_path + 'raw_credit_risk_indicator_data.csv')
            self.raw_credit_risk_indicator_data_recent = pd.read_csv(
                self.data_storage_path + 'raw_credit_risk_indicator_data_recent.csv', parse_dates=['date'])
            self.raw_customer_metric_data = pd.read_csv(self.data_storage_path + 'raw_customer_metric_data.csv',
                                                        parse_dates=['date'])

        self.raw_balance_data['principal_balance'] = self.raw_balance_data['principal_balance'].astype(float)
        self.raw_balance_data['principal_balance_excl_defaults'] = self.raw_balance_data[
            'principal_balance_excl_defaults'].astype(float)
        self.raw_balance_data['revenue'] = self.raw_balance_data['revenue'].astype(float)
        self.raw_balance_data['loan_count'] = self.raw_balance_data['loan_count'].astype(float)
        self.raw_balance_data['avg_interest'] = self.raw_balance_data['avg_interest'].astype(float)
        #        self.raw_balance_data['avg_remaining_term'] = self.raw_balance_data['avg_remaining_term'].astype(float)
        #        self.raw_balance_data['avg_original_term'] = self.raw_balance_data['avg_original_term'].astype(float)

        self.raw_origination_data['principal'] = self.raw_origination_data['principal'].astype(float)
        self.raw_origination_data['loan_count'] = self.raw_origination_data['loan_count'].astype(float)

        self.raw_credit_risk_indicator_data['principal_balance'] = self.raw_credit_risk_indicator_data[
            'principal_balance'].astype(float)
        self.raw_credit_risk_indicator_data['loan_count'] = self.raw_credit_risk_indicator_data['loan_count'].astype(
            float)

        self.raw_credit_risk_indicator_data_recent['principal_balance'] = self.raw_credit_risk_indicator_data_recent[
            'principal_balance'].astype(float)
        self.raw_credit_risk_indicator_data_recent['loan_count'] = self.raw_credit_risk_indicator_data_recent[
            'loan_count'].astype(float)

        self.raw_customer_metric_data['principal_balance'] = self.raw_customer_metric_data['principal_balance'].astype(
            float)
        self.raw_customer_metric_data['loan_count'] = self.raw_customer_metric_data['loan_count'].astype(float)
        self.raw_customer_metric_data['customer_count'] = self.raw_customer_metric_data['customer_count'].astype(float)

    def generate_additional_datasets(self):

        ###########################################################################################################################################################################
        #
        # About generate_additional_datasets()
        #
        # 1. Generates empty output templates containing all dimensions needed for the output, but no values.
        # 2. Genetates daily adjustment factors (as specified by the user in "Settings")
        # 
        ###########################################################################################################################################################################

        date_list_df = pd.DataFrame().assign(date=pd.date_range(start=self.fc_start_date, end=self.fc_end_date))
        date_list = date_list_df['date'].to_list()
        market_list = self.included_countries
        first_cycle_type_list = self.raw_balance_data['first_cycle_type'].drop_duplicates()
        cohort_list = pd.concat([
            self.raw_balance_data['loan_cohort'].drop_duplicates(),
            pd.DataFrame().assign(date=pd.date_range(start=self.fc_start_date, end=self.fc_end_date))[
                'date'].dt.to_period('M').dt.to_timestamp()
        ]).drop_duplicates().to_list()
        country_currency_pairs = self.raw_balance_data[['country_code', 'currency_code']].drop_duplicates()
        country_loan_type_pairs = self.raw_balance_data[['country_code', 'loan_type']].drop_duplicates()

        self.balance_output_template = pd.DataFrame(
            list(itertools.product(date_list, cohort_list, market_list, first_cycle_type_list)),
            columns=['date', 'loan_cohort', 'country_code', 'first_cycle_type']).drop_duplicates().merge(
            country_currency_pairs,
            how='left',
            on=['country_code']
        ).merge(
            country_loan_type_pairs,
            how='left',
            on=['country_code']
        )
        self.balance_output_template = self.balance_output_template[
            self.balance_output_template['date'] >= self.balance_output_template['loan_cohort']]
        self.balance_output_template = self.generate_date_and_age_attributes(self.balance_output_template, 'date',
                                                                             'loan_cohort')

        self.origination_output_template = pd.DataFrame(
            list(itertools.product(date_list, market_list)),
            columns=['date', 'country_code']).drop_duplicates().merge(
            country_currency_pairs,
            how='left',
            on=['country_code']
        ).merge(
            country_loan_type_pairs,
            how='left',
            on=['country_code']
        )
        self.origination_output_template['loan_cohort'] = self.origination_output_template['date'].dt.to_period(
            'M').dt.to_timestamp()
        self.origination_output_template.loc[
            self.origination_output_template['date'].dt.day < 15, 'first_cycle_type'] = 'short'
        self.origination_output_template.loc[
            self.origination_output_template['date'].dt.day >= 15, 'first_cycle_type'] = 'long'
        self.origination_output_template = self.generate_date_and_age_attributes(self.origination_output_template,
                                                                                 'date', 'date')

        self.adjustment_forecast = self.origination_output_template[['date', 'country_code']].drop_duplicates().assign(
            origination_adjustment=1,
            repayment_adjustment=1,
            backbook_cpd_adjustment=1,
            frontbook_cpd_adjustment=1
        )
        for countries in self.adjustments['Country'].drop_duplicates().to_list():

            for dates in self.adjustments[self.adjustments['Country'] == countries].sort_values(by='Apply from date')[
                'Apply from date'].drop_duplicates().to_list():
                self.adjustment_forecast.loc[

                    (self.adjustment_forecast['country_code'] == countries) &
                    (self.adjustment_forecast['date'] >= dates),
                    'origination_adjustment',
                ] = self.adjustments[
                    (self.adjustments['Country'] == countries) &
                    (self.adjustments['Apply from date'] == dates)
                    ]['Origination adjustment'].values[0]

                self.adjustment_forecast.loc[

                    (self.adjustment_forecast['country_code'] == countries) &
                    (self.adjustment_forecast['date'] >= dates),
                    'repayment_adjustment',
                ] = self.adjustments[
                    (self.adjustments['Country'] == countries) &
                    (self.adjustments['Apply from date'] == dates)
                    ]['Repayment adjustment'].values[0]

                self.adjustment_forecast.loc[

                    (self.adjustment_forecast['country_code'] == countries) &
                    (self.adjustment_forecast['date'] >= dates),
                    'backbook_cpd_adjustment',
                ] = self.adjustments[
                    (self.adjustments['Country'] == countries) &
                    (self.adjustments['Apply from date'] == dates)
                    ]['Back-book CPD adjustment'].values[0]

                self.adjustment_forecast.loc[

                    (self.adjustment_forecast['country_code'] == countries) &
                    (self.adjustment_forecast['date'] >= dates),
                    'frontbook_cpd_adjustment',
                ] = self.adjustments[
                    (self.adjustments['Country'] == countries) &
                    (self.adjustments['Apply from date'] == dates)
                    ]['Front-book CPD adjustment'].values[0]

        self.ignored_period_daily = self.raw_balance_data[['date', 'country_code']].drop_duplicates().assign(
            ignore_date=False
        )

        for countries in self.ignored_periods['Country'].drop_duplicates().to_list():

            for start_date in \
                    self.ignored_periods[self.ignored_periods['Country'] == countries].sort_values(
                        by='Ignore period start')[
                        'Ignore period start'].drop_duplicates().to_list():
                end_date = self.ignored_periods[

                    (self.ignored_periods['Country'] == countries) &
                    (self.ignored_periods['Ignore period start'] == start_date)
                    ]['Ignore period end'].values[0]

                self.ignored_period_daily.loc[

                    (self.ignored_period_daily['country_code'] == countries) &
                    (self.ignored_period_daily['date'] >= start_date) &
                    (self.ignored_period_daily['date'] <= end_date),
                    'ignore_date',
                ] = True

    def date_is_holiday(self, date, country_code=None):

        ###########################################################################################################################################################################
        #
        # About date_is_holiday()
        #
        # Checks if a particular date is a bank holiday or not and returns a True or False value accordingly. Works for SE, DE, FI, DE, NO and ES, but more countries can be added.
        # 
        ###########################################################################################################################################################################

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

        ###########################################################################################################################################################################
        #
        # About consider_national_holidays()
        #
        # Updates the column 'weekday_id' to 'Weekend' for dates that are bank holdiays
        # 
        ###########################################################################################################################################################################

        df_dates = df[[date_column_name, country_code_column_name]].drop_duplicates()
        df_dates['is_holiday'] = df_dates.apply(
            lambda x: self.date_is_holiday(x[date_column_name], x[country_code_column_name]), axis=1)

        df = df.merge(
            df_dates,
            how='left',
            on=[date_column_name, country_code_column_name]
        )

        df.loc[df['is_holiday'] == True, 'weekday_id'] = 'Weekend'

        return df.drop(['is_holiday'], axis=1)

    def remove_ignored_dates(self, df):

        df_copy = df.copy()

        df_copy = df_copy.merge(
            self.ignored_period_daily,
            how='left',
            on=['date', 'country_code']
        )

        return df_copy[df_copy['ignore_date'] == False].drop(['ignore_date'], axis=1)

    def generate_date_and_age_attributes(self, df, date_column_name, cohort_column_name):

        ###########################################################################################################################################################################
        #
        # About generate_date_and_age_attributes()
        #
        # Adds 'weekday_id' ('Mon', 'Tue-Fri' or 'Weekend')
        # Adds 'day_id' (part of the month: 'day 1-5', 'day 6-10', 'day 11-15', 'day 16-20', 'day 21-25' or 'day 26-31')
        # Adds 'age_bucket' (age of a particular cohort: '0M', '1M', '2M', '3M', '3-6M', '>6M')
        ###########################################################################################################################################################################        

        # Cohort age buckets

        df['cohort_age_months'] = 12 * (df[date_column_name].dt.year - df[cohort_column_name].dt.year) + (
                df[date_column_name].dt.month - df[cohort_column_name].dt.month)
        bins = [-np.inf, 0, 1, 2, 3, 6, np.inf]
        labels = ['0M', '1M', '2M', '3M', '3-6M', '>6M']
        df['age_bucket'] = pd.cut(df['cohort_age_months'], bins=bins, labels=labels)

        if 'first_cycle_type' in df.columns:
            df_m_one = df[df['age_bucket'] == '1M']
            df_m_one['age_bucket'] = df_m_one['age_bucket'].astype(str) + '_' + df_m_one['first_cycle_type'].astype(str)

            df = pd.concat([
                df_m_one,
                df[df['age_bucket'] != '1M']
            ])

        # Day in month bucket

        bins = [-np.inf, 5, 10, 15, 20, 25, np.inf]
        labels = ['day 1-5', 'day 6-10', 'day 11-15', 'day 16-20', 'day 21-25', 'day 26-31']
        df['day_id'] = pd.cut(df[date_column_name].dt.day, bins=bins, labels=labels)

        # Day in week buckets

        bins = [-np.inf, 0, 4, np.inf]
        labels = ['Mon', 'Tue-Fri', 'Weekend']
        df['weekday_id'] = pd.cut(df[date_column_name].dt.weekday, bins=bins, labels=labels)

        # Treat national holidays like weekends

        df = self.consider_national_holidays(df, date_column_name, 'country_code')

        # Month in year bucket

        df['month_id'] = df[date_column_name].dt.month

        return df.drop(['cohort_age_months'], axis=1).reset_index(drop=True)

    def estimate_balance_growth_factors(self):

        ###########################################################################################################################################################################
        #
        # About estimate_balance_growth_factors()
        #
        # Calculates the average daily % change in balance per cohort. As far as possible, this is done per 'country_code', 'currency_code', 'loan_type', 'month_id', 'day_id', 'age_bucket' and 'weekday_id'
        # For cases when there is no such data, more aggregated calculations are being made by first dropping 'weekday_id' (meaning that weekday-specific patterns are ignored), and if there is still no
        # data, 'loan_type' is being dropped. If there is still no data, 'month_id' is being dropped as well, meaning that seasonal effects throughout the year are being ignored for these cases. 
        #
        ###########################################################################################################################################################################

        model_estimation_data = self.raw_balance_data[
            (self.raw_balance_data['amount_significant'] == True) & (self.raw_balance_data['loan_count'] > 20)][
            ['date', 'loan_cohort', 'first_cycle_type', 'country_code', 'currency_code', 'loan_type',
             'principal_balance', 'loan_count']].groupby([
            'date', 'loan_cohort', 'first_cycle_type', 'country_code', 'currency_code', 'loan_type'
        ]).sum().reset_index()
        model_estimation_data = self.remove_ignored_dates(model_estimation_data)

        model_estimation_data['balance_per_loan'] = model_estimation_data['principal_balance'] / model_estimation_data[
            'loan_count']
        model_estimation_data = model_estimation_data.merge(
            model_estimation_data.assign(date=model_estimation_data.date + pd.DateOffset(days=1)),
            how='left',
            on=['date', 'loan_cohort', 'first_cycle_type', 'country_code', 'currency_code', 'loan_type'],
            suffixes=('', '_prev_day')
        )
        model_estimation_data = self.generate_date_and_age_attributes(model_estimation_data, 'date', 'loan_cohort')

        for new_markets in self.new_markets['Country'].drop_duplicates().to_list():
            behave_like_market = \
                self.new_markets[self.new_markets['Country'] == new_markets]['Behave like country'].values[0]
            new_market_currency = self.new_markets[self.new_markets['Country'] == new_markets]['Currency'].values[0]

            model_estimation_data = pd.concat([

                model_estimation_data[model_estimation_data['country_code'] != new_markets],

                model_estimation_data[model_estimation_data['country_code'] == behave_like_market].assign(
                    country_code=new_markets,
                    currency_code=new_market_currency
                )
            ])

        balance_growth_rates_1 = model_estimation_data.groupby(
            ['country_code', 'currency_code', 'loan_type', 'month_id', 'day_id', 'age_bucket',
             'weekday_id']).sum().reset_index()
        balance_growth_rates_1['balance_growth_rate'] = (
                balance_growth_rates_1['balance_per_loan'] / balance_growth_rates_1[
            'balance_per_loan_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_1['loan_growth_rate'] = (
                balance_growth_rates_1['loan_count'] / balance_growth_rates_1['loan_count_prev_day']).replace(
            [-np.inf, np.inf], np.nan)
        balance_growth_rates_1 = balance_growth_rates_1.drop(
            ['balance_per_loan', 'balance_per_loan_prev_day', 'loan_count', 'loan_count_prev_day', 'principal_balance',
             'principal_balance_prev_day'], axis=1)

        balance_growth_rates_2 = model_estimation_data.groupby(
            ['country_code', 'currency_code', 'loan_type', 'month_id', 'day_id', 'age_bucket']).sum().reset_index()
        balance_growth_rates_2['balance_growth_rate'] = (
                balance_growth_rates_2['balance_per_loan'] / balance_growth_rates_2[
            'balance_per_loan_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_2['loan_growth_rate'] = (
                balance_growth_rates_2['loan_count'] / balance_growth_rates_2['loan_count_prev_day']).replace(
            [-np.inf, np.inf], np.nan)
        balance_growth_rates_2 = balance_growth_rates_2.drop(
            ['balance_per_loan', 'balance_per_loan_prev_day', 'loan_count', 'loan_count_prev_day', 'principal_balance',
             'principal_balance_prev_day'], axis=1)

        balance_growth_rates_3 = model_estimation_data.groupby(
            ['country_code', 'currency_code', 'month_id', 'day_id', 'age_bucket']).sum().reset_index()
        balance_growth_rates_3['balance_growth_rate'] = (
                balance_growth_rates_3['balance_per_loan'] / balance_growth_rates_3[
            'balance_per_loan_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_3['loan_growth_rate'] = (
                balance_growth_rates_3['loan_count'] / balance_growth_rates_3['loan_count_prev_day']).replace(
            [-np.inf, np.inf], np.nan)
        balance_growth_rates_3 = balance_growth_rates_3.drop(
            ['balance_per_loan', 'balance_per_loan_prev_day', 'loan_count', 'loan_count_prev_day', 'principal_balance',
             'principal_balance_prev_day'], axis=1)

        balance_growth_rates_4 = model_estimation_data.groupby(
            ['country_code', 'currency_code', 'day_id', 'age_bucket']).sum().reset_index()
        balance_growth_rates_4['balance_growth_rate'] = (
                balance_growth_rates_4['balance_per_loan'] / balance_growth_rates_4[
            'balance_per_loan_prev_day']).replace([-np.inf, np.inf], np.nan)
        balance_growth_rates_4['loan_growth_rate'] = (
                balance_growth_rates_4['loan_count'] / balance_growth_rates_4['loan_count_prev_day']).replace(
            [-np.inf, np.inf], np.nan)
        balance_growth_rates_4 = balance_growth_rates_4.drop(
            ['balance_per_loan', 'balance_per_loan_prev_day', 'loan_count', 'loan_count_prev_day', 'principal_balance',
             'principal_balance_prev_day', 'month_id'], axis=1)

        self.growth_rates = self.balance_output_template.merge(
            balance_growth_rates_1,
            how='left',
            on=['country_code', 'currency_code', 'loan_type', 'month_id', 'day_id', 'age_bucket', 'weekday_id']
        ).merge(
            balance_growth_rates_2,
            how='left',
            on=['country_code', 'currency_code', 'loan_type', 'month_id', 'day_id', 'age_bucket'],
            suffixes=('', '_backup')
        ).merge(
            balance_growth_rates_3,
            how='left',
            on=['country_code', 'currency_code', 'month_id', 'day_id', 'age_bucket'],
            suffixes=('', '_backup_2')
        ).merge(
            balance_growth_rates_4,
            how='left',
            on=['country_code', 'currency_code', 'day_id', 'age_bucket'],
            suffixes=('', '_backup_3')
        )

        self.growth_rates['balance_growth_rate'] = self.growth_rates['balance_growth_rate'].fillna(
            self.growth_rates['balance_growth_rate_backup']).fillna(
            self.growth_rates['balance_growth_rate_backup_2']).fillna(self.growth_rates['balance_growth_rate_backup_3'])
        self.growth_rates['loan_growth_rate'] = self.growth_rates['loan_growth_rate'].fillna(
            self.growth_rates['loan_growth_rate_backup']).fillna(self.growth_rates['loan_growth_rate_backup_2']).fillna(
            self.growth_rates['loan_growth_rate_backup_3'])
        self.growth_rates = self.growth_rates.drop(
            ['balance_growth_rate_backup', 'loan_growth_rate_backup', 'balance_growth_rate_backup_2',
             'loan_growth_rate_backup_2', 'balance_growth_rate_backup_3', 'loan_growth_rate_backup_3'], axis=1)
        self.growth_rates = self.growth_rates.merge(
            self.adjustment_forecast[['date', 'country_code', 'repayment_adjustment']],
            how='left',
            on=['date', 'country_code']
        )
        self.growth_rates.loc[(self.growth_rates['age_bucket'] != '0M'), 'balance_growth_rate'] = (1 + (
                self.growth_rates['balance_growth_rate'] * self.growth_rates['loan_growth_rate'] - 1) *
                                                                                                   self.growth_rates[
                                                                                                       'repayment_adjustment']) / \
                                                                                                  self.growth_rates[
                                                                                                      'loan_growth_rate']
        self.growth_rates = self.growth_rates.drop(['repayment_adjustment'], axis=1)

    def generate_pre_current_cohort_backbook_forecast(self):

        ##########################################################################################################################################################################
        #
        # About generate_backbook_forecast()
        # Starts from balances as of the last month before the forecast start date and extrapolates forecast balances for all existing cohorts (i.e. backbook).
        # No new cohorts are considered in this section.
        #
        ###########################################################################################################################################################################

        starting_point = self.generate_date_and_age_attributes(
            self.raw_balance_data[(self.raw_balance_data['date'] < self.fc_start_date) & (
                    self.raw_balance_data['loan_cohort'] < dt.datetime(self.fc_start_date.year,
                                                                       self.fc_start_date.month, 1))],
            'date',
            'loan_cohort'
        )[self.balance_output_template.columns.to_list() + ['creditor_id', 'principal_balance', 'loan_count']]
        starting_point = starting_point[starting_point['date'] == starting_point['date'].max()]
        starting_point = starting_point.groupby(
            ['date', 'loan_cohort', 'first_cycle_type', 'country_code', 'loan_type', 'creditor_id',
             'currency_code']).sum().reset_index()
        starting_point['balance_per_loan'] = starting_point['principal_balance'] / starting_point['loan_count']
        starting_point['data_source'] = 'actual'

        print('Generating backbook forecast (pre current cohort)...')

        self.backbook_forecast = self.generate_new_days(starting_point, 'backbook_forecast')
        self.backbook_forecast = self.backbook_forecast[self.backbook_forecast['data_source'] != 'actual']

    def generate_new_days(self, days_up_until_yesterday, data_source_name=None):

        ###########################################################################################################################################################################
        #
        # About generate_new_days()
        #
        # Starts off from an existing dataset and creates the next day of the forecast. This module is being used to create both the backbook- and frontbookforecasts.
        #
        ###########################################################################################################################################################################

        last_date = days_up_until_yesterday['date'].copy().max()

        if last_date == self.fc_end_date:

            return days_up_until_yesterday

        else:

            new_day_data = days_up_until_yesterday[days_up_until_yesterday['date'] == last_date]
            new_day_data['date'] = new_day_data['date'] + pd.DateOffset(days=1)
            new_day_data = self.generate_date_and_age_attributes(
                new_day_data,
                'date',
                'loan_cohort'
            )

            new_day_data = new_day_data.merge(
                self.growth_rates,
                how='left',
                on=self.balance_output_template.columns.to_list()
            )
            new_day_data['balance_per_loan'] *= new_day_data['balance_growth_rate']
            new_day_data['loan_count'] *= new_day_data['loan_growth_rate']
            new_day_data['principal_balance'] = new_day_data['balance_per_loan'] * new_day_data['loan_count']
            new_day_data = new_day_data.drop(['balance_growth_rate', 'loan_growth_rate'], axis=1)

            if data_source_name is not None:
                new_day_data['data_source'] = data_source_name

            return self.generate_new_days(pd.concat([days_up_until_yesterday, new_day_data]))

    def generate_first_month_balance_to_origination_ratios(self):

        ###########################################################################################################################################################################
        #
        # About generate_first_month_balance_to_origination_ratios()
        #
        # Calculates the average historical ratio between cumulative origination and balances during the first month of a cohort. These ratios should be close to 1 (as there should be very
        # small repayments within the first month since origination).
        #
        ###########################################################################################################################################################################

        first_month_balances = self.raw_balance_data[

            (self.raw_balance_data['months_since'] == 0) &
            (self.raw_balance_data['first_cycle_type'] == 'short')

            ][['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code', 'principal_balance',
               'loan_count']].groupby(
            ['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code']).sum().reset_index()

        cumulative_origination_data = self.raw_origination_data[
            (self.raw_origination_data['first_cycle_type'] == 'short')
        ].groupby(['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code']).sum().sort_values(
            by='date').groupby(['loan_cohort', 'loan_type', 'country_code', 'currency_code']).cumsum().reset_index()

        model_estimation_data = first_month_balances.merge(
            cumulative_origination_data.rename(
                columns={'principal': 'cum_originated_amount', 'loan_count': 'cum_loan_count'}),
            how='left',
            on=['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code']
        ).sort_values(by='date')
        model_estimation_data = self.remove_ignored_dates(model_estimation_data)
        model_estimation_data['cum_originated_amount'] = \
            model_estimation_data.groupby(['loan_cohort', 'loan_type', 'country_code', 'currency_code'])[
                'cum_originated_amount'].transform(lambda v: v.ffill())
        model_estimation_data['cum_loan_count'] = \
            model_estimation_data.groupby(['loan_cohort', 'loan_type', 'country_code', 'currency_code'])[
                'cum_loan_count'].transform(lambda v: v.ffill())
        model_estimation_data['day_id'] = model_estimation_data['date'].dt.day

        for new_markets in self.new_markets['Country'].drop_duplicates().to_list():
            behave_like_market = \
                self.new_markets[self.new_markets['Country'] == new_markets]['Behave like country'].values[0]
            new_market_currency = self.new_markets[self.new_markets['Country'] == new_markets]['Currency'].values[0]

            model_estimation_data = pd.concat([

                model_estimation_data[model_estimation_data['country_code'] != new_markets],

                model_estimation_data[model_estimation_data['country_code'] == behave_like_market].assign(
                    country_code=new_markets,
                    currency_code=new_market_currency
                )
            ])

        self.balance_to_origination_ratios = model_estimation_data.groupby(
            ['loan_type', 'country_code', 'currency_code', 'day_id']).sum().reset_index()
        self.balance_to_origination_ratios['cum_balance_ratio'] = self.balance_to_origination_ratios[
                                                                      'principal_balance'] / \
                                                                  self.balance_to_origination_ratios[
                                                                      'cum_originated_amount']
        self.balance_to_origination_ratios['cum_loan_ratio'] = self.balance_to_origination_ratios['loan_count'] / \
                                                               self.balance_to_origination_ratios['cum_loan_count']
        self.balance_to_origination_ratios = self.balance_to_origination_ratios.drop(
            ['loan_count', 'cum_loan_count', 'principal_balance', 'cum_originated_amount'], axis=1)

    def generate_new_vs_old_customer_split(self):

        ###########################################################################################################################################################################
        #
        # About generate_new_vs_old_customer_split()
        #
        # Estimates the share of new vs. existing customers origination volumes. The increasing trend in the % share of existing customers is being incorporated and estimated in the forecast.
        # This is being done by least square fitting the parameters a and b in the function 'existing_customer_share_function(t, a, b)' described separately.
        #
        ###########################################################################################################################################################################

        template = self.raw_origination_data[['date', 'country_code', 'currency_code', 'loan_type']].drop_duplicates()
        new_customer_obs = self.raw_origination_data[self.raw_origination_data['origination_type'] == 'new_customer'][
            ['date', 'country_code', 'currency_code', 'loan_type', 'loan_count']].groupby([
            'date', 'country_code', 'currency_code', 'loan_type'
        ]).sum().reset_index()
        all_customer_obs = self.raw_origination_data[
            ['date', 'country_code', 'currency_code', 'loan_type', 'loan_count']].groupby([
            'date', 'country_code', 'currency_code', 'loan_type'
        ]).sum().reset_index()

        customer_split_obs = template.merge(
            all_customer_obs,
            how='left',
            on=['date', 'country_code', 'currency_code', 'loan_type']
        ).merge(
            new_customer_obs,
            how='left',
            on=['date', 'country_code', 'currency_code', 'loan_type'],
            suffixes=('', '_new')
        )
        customer_split_obs['new_customer_share'] = (
                customer_split_obs['loan_count_new'] / customer_split_obs['loan_count']).replace([-np.inf, np.inf],
                                                                                                 np.nan).fillna(0)
        customer_split_obs = customer_split_obs.drop(['loan_count', 'loan_count_new'], axis=1)

        self.customer_split_fc = self.origination_output_template[
            ['date', 'country_code', 'currency_code', 'loan_type', 'loan_cohort']].drop_duplicates()

        for markets in customer_split_obs['country_code'].drop_duplicates().to_list():
            for loan_type in customer_split_obs[customer_split_obs['country_code'] == markets][
                'loan_type'].drop_duplicates().to_list():
                customer_split_obs_i = customer_split_obs[
                    (customer_split_obs['country_code'] == markets) & (customer_split_obs['loan_type'] == loan_type)]
                base_date_i = customer_split_obs_i['date'].min()
                customer_split_obs_i['base_date'] = base_date_i
                customer_split_obs_i['t'] = (customer_split_obs_i['date'] - customer_split_obs_i['base_date']).dt.days

                a_i, b_i = curve_fit(

                    self.existing_customer_share_function,
                    customer_split_obs_i['t'].to_list(),
                    customer_split_obs_i['new_customer_share'].to_list(),
                    bounds=[(0, 0), (np.inf, np.inf)]

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
        self.customer_split_fc = self.customer_split_fc.assign(
            new_customer_share=lambda x: self.existing_customer_share_function(x['t'], x['a'], x['b']))
        self.customer_split_fc = self.customer_split_fc.drop(['a', 'b', 't', 'base_date'], axis=1)

    def existing_customer_share_function(self, t, a, b):

        ###########################################################################################################################################################################
        #
        # About existing_customer_share_function()
        #
        # The mathematical function f(t) with parameters a and b that is assumed to describe the share of existing customers. t here indicates the number of days
        # between a base date and the current date.
        #
        ###########################################################################################################################################################################

        return a / (a + b * t)

    def generate_creditor_distribution_new_customers(self):

        ###########################################################################################################################################################################
        #
        # About generate_creditor_distribution_new_customers()
        # 
        # Generates the daily creditor distribution for new customers based on the input assumptions made in 'Settings' by the user. Creditor distribution here refers to the share of new customers
        # (no previous volumes with Anyfin) that is being allocated to each creditor.
        #
        ###########################################################################################################################################################################

        date_list = self.origination_output_template['date'].drop_duplicates().to_list()
        country_list = self.origination_output_template['country_code'].drop_duplicates().to_list()
        country_creditor_pairs = self.creditor_split_new_customers[['Country', 'Creditor']].drop_duplicates()

        self.creditor_split_fc = pd.DataFrame(
            list(itertools.product(date_list, country_list)),
            columns=['date', 'country_code']).drop_duplicates().merge(
            country_creditor_pairs.rename(columns={'Country': 'country_code', 'Creditor': 'creditor_id'}),
            how='left',
            on=['country_code']
        )

        for creditors in self.creditor_split_new_customers['Creditor'].drop_duplicates().to_list():

            for dates in \
                    self.creditor_split_new_customers[
                        self.creditor_split_new_customers['Creditor'] == creditors].sort_values(
                        by='Apply from date')['Apply from date'].drop_duplicates().to_list():
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

        self.creditor_split_fc['creditor_share_new_customers'] = self.creditor_split_fc[
            'creditor_share_new_customers'].fillna(0)
        self.creditor_split_fc = self.creditor_split_fc[self.creditor_split_fc['creditor_share_new_customers'] > 0]

    def generate_monthly_origination_growth_rates(self):

        ###########################################################################################################################################################################
        #
        # About generate_monthly_origination_growth_rates()
        # 
        # Calculates average historical monthly origination growth rates per market and month_id (Jan, Feb,...Dec). For cases when there is no such data, a more aggregated calculation
        # without month_id is being made and used as a proxy.
        #
        ###########################################################################################################################################################################

        model_monthly_estimation_data = self.remove_ignored_dates(self.raw_origination_data)
        model_monthly_estimation_data = model_monthly_estimation_data.groupby(
            ['loan_cohort', 'country_code', 'currency_code']).sum().reset_index()
        model_monthly_estimation_data = model_monthly_estimation_data[
            model_monthly_estimation_data['loan_cohort'] < self.fc_start_date.replace(day=1)]
        model_monthly_estimation_data = model_monthly_estimation_data.merge(
            model_monthly_estimation_data.assign(
                loan_cohort=model_monthly_estimation_data.loan_cohort + pd.DateOffset(months=1)),
            how='left',
            on=['loan_cohort', 'country_code', 'currency_code'],
            suffixes=('', '_last_month')
        )
        model_monthly_estimation_data['month_id'] = model_monthly_estimation_data['loan_cohort'].dt.month
        model_monthly_estimation_data = model_monthly_estimation_data[(model_monthly_estimation_data[
                                                                           'loan_cohort'] >= self.fc_start_date.replace(
            day=1) - pd.DateOffset(months=14)) & (model_monthly_estimation_data['loan_count'] > 1000)]
        monthly_growth_1 = model_monthly_estimation_data.groupby(
            ['month_id', 'country_code', 'currency_code']).sum().reset_index()
        monthly_growth_1['loan_growth'] = (monthly_growth_1['loan_count'] / monthly_growth_1['loan_count_last_month'])
        monthly_growth_1['balance_growth'] = (monthly_growth_1['principal'] / monthly_growth_1['loan_count']) / (
                monthly_growth_1['principal_last_month'] / monthly_growth_1['loan_count_last_month'])
        monthly_growth_1 = monthly_growth_1.drop(
            ['principal', 'loan_count', 'principal_last_month', 'loan_count_last_month'], axis=1)

        monthly_growth_2 = model_monthly_estimation_data.groupby(
            ['country_code', 'currency_code']).sum().reset_index().drop(['month_id'], axis=1)
        monthly_growth_2['loan_growth'] = (monthly_growth_2['loan_count'] / monthly_growth_2['loan_count_last_month'])
        monthly_growth_2['balance_growth'] = (monthly_growth_2['principal'] / monthly_growth_2['loan_count']) / (
                monthly_growth_2['principal_last_month'] / monthly_growth_2['loan_count_last_month'])
        monthly_growth_2 = monthly_growth_2.drop(
            ['principal', 'loan_count', 'principal_last_month', 'loan_count_last_month'], axis=1)

        self.origination_monthly_growth_rates = self.origination_output_template[
            ['loan_cohort', 'country_code', 'currency_code', 'loan_type', 'month_id']].drop_duplicates().merge(
            monthly_growth_1,
            how='left',
            on=['month_id', 'country_code', 'currency_code']
        ).merge(
            monthly_growth_2,
            how='left',
            on=['country_code', 'currency_code'],
            suffixes=('', '_2')
        )
        self.origination_monthly_growth_rates['balance_growth'] = self.origination_monthly_growth_rates[
            'balance_growth'].replace([-np.inf, np.inf], np.nan).fillna(
            self.origination_monthly_growth_rates['balance_growth_2']).replace([-np.inf, np.inf], np.nan).fillna(1)
        self.origination_monthly_growth_rates['loan_growth'] = self.origination_monthly_growth_rates[
            'loan_growth'].replace([-np.inf, np.inf], np.nan).fillna(
            self.origination_monthly_growth_rates['loan_growth_2']).replace([-np.inf, np.inf], np.nan).fillna(1)
        self.origination_monthly_growth_rates = self.origination_monthly_growth_rates.drop(
            ['balance_growth_2', 'loan_growth_2'], axis=1)

        for new_markets in self.new_markets['Country'].drop_duplicates().to_list():
            behave_like_market = \
                self.new_markets[self.new_markets['Country'] == new_markets]['Behave like country'].values[0]
            new_market_currency = self.new_markets[self.new_markets['Country'] == new_markets]['Currency'].values[0]

            self.origination_monthly_growth_rates = pd.concat([

                self.origination_monthly_growth_rates[
                    self.origination_monthly_growth_rates['country_code'] != new_markets],

                self.origination_monthly_growth_rates[
                    self.origination_monthly_growth_rates['country_code'] == behave_like_market].assign(
                    country_code=new_markets,
                    currency_code=new_market_currency,
                )
            ])

    def generate_monthly_origination_forecast(self):

        ###########################################################################################################################################################################
        #
        # About generate_monthly_origination_forecast()
        # 
        # For given monthly growth rates, this module calculates future origination volumes per month  by starting off from the level on the month before the forecast start date and
        # extrapolating from there.
        #
        ###########################################################################################################################################################################

        print('Generating monthly origination forecast...')

        starting_point = self.raw_origination_data[
            self.raw_origination_data['loan_cohort'] < self.fc_start_date.replace(day=1)]
        starting_point = starting_point.groupby(
            ['loan_cohort', 'loan_type', 'country_code', 'currency_code']).sum().reset_index()
        starting_point['principal_per_loan'] = starting_point['principal'] / starting_point['loan_count']
        starting_point['data_source'] = 'actual'

        self.monthly_origination_forecast = self.generate_future_origination_months(starting_point)
        self.monthly_origination_forecast = self.monthly_origination_forecast.drop(['principal_per_loan'], axis=1)

    def estimate_intra_month_origination_profile(self):

        ###########################################################################################################################################################################
        #
        # About estimate_intra_month_origination_profile()
        # 
        # Calculates the daily % share of a month total origination. All shares in a specific month sums to 100%.
        #
        ###########################################################################################################################################################################

        estimation_data = self.remove_ignored_dates(self.raw_origination_data)
        daily_origination = estimation_data[['date', 'loan_cohort', 'loan_type', 'country_code', 'loan_count']].groupby(
            ['date', 'loan_cohort', 'loan_type', 'country_code']).sum().reset_index()
        monthly_origination = estimation_data[
            ['date', 'loan_cohort', 'loan_type', 'country_code', 'loan_count']].groupby(
            ['loan_cohort', 'loan_type', 'country_code']).sum().reset_index()

        monthly_profiles = self.generate_date_and_age_attributes(
            daily_origination.merge(
                monthly_origination,
                how='left',
                on=['loan_cohort', 'loan_type', 'country_code'],
                suffixes=('', '_month_tot')
            ),
            'date',
            'loan_cohort'
        )

        monthly_profiles['monthly_share'] = monthly_profiles['loan_count'] / monthly_profiles['loan_count_month_tot']
        monthly_profiles['weighted_monthly_share_i'] = monthly_profiles['monthly_share'] * monthly_profiles[
            'loan_count']
        monthly_profiles = monthly_profiles[(monthly_profiles['date'].dt.year >= self.fc_start_date.year - 2) & (
                monthly_profiles['date'] <= self.fc_start_date.replace(day=1))]

        monthly_profiles_agg = monthly_profiles.groupby(
            ['day_id', 'weekday_id', 'country_code', 'loan_type']).sum().reset_index()
        monthly_profiles_agg['average_share'] = (
                monthly_profiles_agg['weighted_monthly_share_i'] / monthly_profiles_agg['loan_count']).fillna(0)
        monthly_profiles_agg = monthly_profiles_agg.drop(
            ['loan_count', 'loan_count_month_tot', 'month_id', 'monthly_share', 'weighted_monthly_share_i'], axis=1)

        template = self.origination_output_template[
            ['date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type', 'day_id',
             'weekday_id']].drop_duplicates()

        self.intra_month_shares = template.merge(
            monthly_profiles_agg,
            how='left',
            on=['day_id', 'weekday_id', 'country_code', 'loan_type']
        )
        self.intra_month_shares['average_share'] = self.intra_month_shares['average_share'].fillna(0)
        self.intra_month_shares = self.intra_month_shares.merge(
            self.intra_month_shares.groupby(
                ['loan_cohort', 'country_code', 'currency_code', 'loan_type']).sum().reset_index(),
            how='left',
            on=['loan_cohort', 'country_code', 'currency_code', 'loan_type'],
            suffixes=('', '_month_tot')
        )
        self.intra_month_shares['average_share'] /= self.intra_month_shares['average_share_month_tot']
        self.intra_month_shares = self.intra_month_shares.drop(['average_share_month_tot'], axis=1)
        share_of_start_month = ((self.fc_start_date.replace(month=self.fc_start_date.month % 12 + 1,
                                                            day=1) - pd.DateOffset(
            days=1)).day - self.fc_start_date.day) / (
                                       self.fc_start_date.replace(month=self.fc_start_date.month % 12 + 1,
                                                                  day=1) - pd.DateOffset(days=1)).day
        self.intra_month_shares.loc[self.intra_month_shares['loan_cohort'] == self.fc_start_date.replace(
            day=1), 'average_share'] *= share_of_start_month

    def generate_daily_origination_forecast(self):

        ###########################################################################################################################################################################
        #
        # About generate_daily_origination_forecast()
        # 
        # For given forecast of monthly origination volumes and intra month shares, this module calcualtes daily origination forecast volumes.
        #
        ###########################################################################################################################################################################

        print('Splitting monthly origination to daily forecast...')

        self.daily_origination_forecast = self.intra_month_shares.merge(
            self.monthly_origination_forecast,
            how='left',
            on=['loan_cohort', 'country_code', 'currency_code', 'loan_type']
        )
        self.daily_origination_forecast['principal'] *= self.daily_origination_forecast['average_share']
        self.daily_origination_forecast['loan_count'] *= self.daily_origination_forecast['average_share']
        self.daily_origination_forecast = self.daily_origination_forecast.drop(['average_share'], axis=1)
        self.daily_origination_forecast = self.daily_origination_forecast.merge(
            self.customer_split_fc,
            how='left',
            on=['date', 'loan_cohort', 'country_code', 'currency_code', 'loan_type']
        )
        self.daily_origination_forecast = pd.concat([

            self.daily_origination_forecast.assign(

                loan_count=self.daily_origination_forecast['loan_count'] * self.daily_origination_forecast[
                    'new_customer_share'],
                principal=self.daily_origination_forecast['principal'] * self.daily_origination_forecast[
                    'new_customer_share'],
                origination_type='new_customer'

            ),

            self.daily_origination_forecast.assign(

                loan_count=self.daily_origination_forecast['loan_count'] * (
                        1 - self.daily_origination_forecast['new_customer_share']),
                principal=self.daily_origination_forecast['principal'] * (
                        1 - self.daily_origination_forecast['new_customer_share']),
                origination_type='existing_customer'

            )

        ]).drop(['new_customer_share'], axis=1)

        daily_origination_forecast_new_customers = self.daily_origination_forecast[
            self.daily_origination_forecast['origination_type'] == 'new_customer'].merge(
            self.creditor_split_fc,
            how='left',
            on=['date', 'country_code']
        )
        daily_origination_forecast_new_customers['principal'] *= daily_origination_forecast_new_customers[
            'creditor_share_new_customers']
        daily_origination_forecast_new_customers['loan_count'] *= daily_origination_forecast_new_customers[
            'creditor_share_new_customers']
        daily_origination_forecast_new_customers = daily_origination_forecast_new_customers.drop(
            ['creditor_share_new_customers'], axis=1)

        self.daily_origination_forecast = pd.concat([

            self.daily_origination_forecast[self.daily_origination_forecast['origination_type'] == 'existing_customer'],
            daily_origination_forecast_new_customers

        ])
        self.daily_origination_forecast.loc[self.daily_origination_forecast[
                                                'origination_type'] == 'existing_customer', 'creditor_id'] = 'creditor_mix_of_current_portfolio'
        self.daily_origination_forecast = self.daily_origination_forecast.merge(
            self.adjustment_forecast[['date', 'country_code', 'origination_adjustment']],
            how='left',
            on=['date', 'country_code']
        )
        self.daily_origination_forecast['loan_count'] *= self.daily_origination_forecast['origination_adjustment']
        self.daily_origination_forecast['principal'] *= self.daily_origination_forecast['origination_adjustment']
        self.daily_origination_forecast = self.daily_origination_forecast.drop(['origination_adjustment'], axis=1)
        self.daily_origination_forecast['first_cycle_type'] = self.daily_origination_forecast.apply(
            lambda x: self.generate_first_cycle_type(x.date, x.country_code), axis=1)

    def generate_first_cycle_type(self, date, country):

        if country == 'SE':

            if date.day <= 13:
                return 'short'
            else:
                return 'long'

        if country == 'DE':

            if date.day <= 19:
                return 'short'
            else:
                return 'long'

        if country == 'FI':

            if date.day <= 14:
                return 'short'
            else:
                return 'long'

        else:

            if date.day <= 15:
                return 'short'
            else:
                return 'long'

    def generate_future_origination_months(self, months_up_until_last_month):

        ###########################################################################################################################################################################
        #
        # About generate_future_origination_months()
        # 
        # For a given starting point, this module generates the next months' origination volumes. Used by the generate_monthly_origination_forecast() module.
        #
        ###########################################################################################################################################################################

        if months_up_until_last_month['loan_cohort'].max() == self.fc_end_date.replace(day=1):

            return months_up_until_last_month

        else:

            new_month_data = months_up_until_last_month[
                months_up_until_last_month['loan_cohort'] == months_up_until_last_month['loan_cohort'].max()]
            new_month_data['loan_cohort'] = new_month_data['loan_cohort'] + pd.DateOffset(months=1)

            new_month_data = new_month_data.merge(
                self.origination_monthly_growth_rates[
                    ['loan_cohort', 'country_code', 'currency_code', 'loan_type', 'balance_growth', 'loan_growth']],
                how='left',
                on=['loan_cohort', 'country_code', 'currency_code', 'loan_type']
            )
            new_month_data['principal_per_loan'] *= new_month_data['balance_growth']
            new_month_data['loan_count'] *= new_month_data['loan_growth']
            new_month_data['principal'] = new_month_data['principal_per_loan'] * new_month_data['loan_count']
            new_month_data = new_month_data.drop(['balance_growth', 'loan_growth'], axis=1)
            new_month_data['data_source'] = 'forecast'

            return pd.concat([

                self.generate_future_origination_months(new_month_data),
                months_up_until_last_month

            ])

    def generate_current_cohort_backbook_forecast(self):

        if self.fc_start_date == self.fc_first_full_month_start_date:

            self.current_cohort_frontbook_forecast = pd.DataFrame()

        else:

            starting_point = self.raw_balance_data[
                (self.raw_balance_data['date'] < self.fc_start_date) &
                (self.raw_balance_data['loan_cohort'] == dt.datetime(self.fc_start_date.year, self.fc_start_date.month,
                                                                     1))
                ]
            starting_point = starting_point[
                ['date', 'loan_cohort', 'first_cycle_type', 'country_code', 'currency_code', 'loan_type', 'creditor_id',
                 'principal_balance', 'loan_count']].groupby([
                'date', 'loan_cohort', 'first_cycle_type', 'country_code', 'currency_code', 'loan_type', 'creditor_id'
            ]).sum().reset_index()
            starting_point = starting_point[starting_point['date'] == starting_point['date'].max()]
            full_month_fc = starting_point.assign(data_source='backbook_forecast')

            country_currency_pairs = full_month_fc[['country_code', 'currency_code']].drop_duplicates()
            country_creditor_pairs = full_month_fc[['country_code', 'creditor_id']].drop_duplicates()
            dates = pd.concat([
                starting_point['date'],
                self.balance_output_template[
                    (self.balance_output_template['date'].dt.year == self.fc_start_date.year) &
                    (self.balance_output_template['date'].dt.month == self.fc_start_date.month)
                    ]['date'].drop_duplicates()
            ]).drop_duplicates().to_list()

            self.balance_output_template[
                (self.balance_output_template['date'].dt.year == self.fc_start_date.year) &
                (self.balance_output_template['date'].dt.month == self.fc_start_date.month)
                ]['date'].drop_duplicates().to_list()

            cohorts = full_month_fc['loan_cohort'].drop_duplicates().to_list()
            loan_types = full_month_fc['loan_type'].drop_duplicates().to_list()
            countries = full_month_fc['country_code'].drop_duplicates().to_list()
            data_source_list = full_month_fc['data_source'].drop_duplicates().to_list()
            first_cycle_type_list = full_month_fc['first_cycle_type'].drop_duplicates().to_list()

            full_month_fc = pd.DataFrame(
                list(itertools.product(dates, cohorts, countries, loan_types, data_source_list, first_cycle_type_list)),
                columns=['date', 'loan_cohort', 'country_code', 'loan_type', 'data_source',
                         'first_cycle_type']).drop_duplicates().merge(
                country_currency_pairs,
                how='left',
                on=['country_code']
            ).merge(
                country_creditor_pairs,
                how='left',
                on=['country_code']
            ).merge(
                full_month_fc,
                how='left',
                on=['date', 'loan_cohort', 'first_cycle_type', 'country_code', 'currency_code', 'loan_type',
                    'creditor_id', 'data_source']
            )
            full_month_fc['principal_balance'] = full_month_fc['principal_balance'].fillna(0)
            full_month_fc['loan_count'] = full_month_fc['loan_count'].fillna(0)
            full_month_fc = full_month_fc.groupby(
                ['date', 'loan_cohort', 'first_cycle_type', 'loan_type', 'country_code', 'currency_code', 'creditor_id',
                 'data_source']).sum().sort_values(by='date').groupby([
                'loan_cohort', 'first_cycle_type', 'loan_type', 'country_code', 'currency_code', 'creditor_id',
                'data_source'
            ]).cumsum().reset_index()
            full_month_fc['principal_balance'] = full_month_fc.groupby([
                'loan_cohort', 'loan_type', 'country_code', 'currency_code', 'creditor_id'
            ])['principal_balance'].transform(lambda v: v.ffill())
            full_month_fc = full_month_fc[full_month_fc['date'] >= self.fc_start_date]

            start_day_in_month = full_month_fc['date'].min().day

            adjusted_balance_to_loan_ratios = self.balance_to_origination_ratios.merge(
                self.balance_to_origination_ratios[
                    self.balance_to_origination_ratios['day_id'] == start_day_in_month].drop(['day_id'], axis=1),
                how='left',
                on=['loan_type', 'country_code', 'currency_code'],
                suffixes=('', '_base_value')
            )

            adjusted_balance_to_loan_ratios['cum_balance_ratio'] /= adjusted_balance_to_loan_ratios[
                'cum_balance_ratio_base_value']
            adjusted_balance_to_loan_ratios['cum_loan_ratio'] /= adjusted_balance_to_loan_ratios[
                'cum_loan_ratio_base_value']
            adjusted_balance_to_loan_ratios = adjusted_balance_to_loan_ratios.drop(
                ['cum_loan_ratio_base_value', 'cum_balance_ratio_base_value'], axis=1)
            adjusted_balance_to_loan_ratios = adjusted_balance_to_loan_ratios[
                adjusted_balance_to_loan_ratios['day_id'] >= self.fc_start_date.day]
            adjusted_balance_to_loan_ratios['cum_balance_ratio'] = adjusted_balance_to_loan_ratios[
                'cum_balance_ratio'].clip(0, 1)
            adjusted_balance_to_loan_ratios['cum_loan_ratio'] = adjusted_balance_to_loan_ratios['cum_loan_ratio'].clip(
                0, 1)

            self.current_cohort_frontbook_forecast = full_month_fc.assign(day_id=full_month_fc['date'].dt.day).merge(
                adjusted_balance_to_loan_ratios,
                how='left',
                on=['loan_type', 'country_code', 'currency_code', 'day_id']
            )
            self.current_cohort_frontbook_forecast.loc[
                self.current_cohort_frontbook_forecast['first_cycle_type'] == 'long', 'cum_balance_ratio'] = 1
            self.current_cohort_frontbook_forecast.loc[
                self.current_cohort_frontbook_forecast['first_cycle_type'] == 'long', 'cum_loan_ratio'] = 1
            self.current_cohort_frontbook_forecast['principal_balance'] *= self.current_cohort_frontbook_forecast[
                'cum_balance_ratio']
            self.current_cohort_frontbook_forecast['loan_count'] *= self.current_cohort_frontbook_forecast[
                'cum_loan_ratio']
            self.current_cohort_frontbook_forecast['balance_per_loan'] = self.current_cohort_frontbook_forecast[
                                                                             'principal_balance'] / \
                                                                         self.current_cohort_frontbook_forecast[
                                                                             'loan_count']
            self.current_cohort_frontbook_forecast = self.current_cohort_frontbook_forecast.drop(
                ['cum_balance_ratio', 'cum_loan_ratio'], axis=1)
            self.current_cohort_frontbook_forecast = self.generate_date_and_age_attributes(
                self.current_cohort_frontbook_forecast,
                'date',
                'loan_cohort'
            )
            self.current_cohort_frontbook_forecast = self.current_cohort_frontbook_forecast[
                self.current_cohort_frontbook_forecast['loan_count'] > 0]

            print('Generating cohort: ' + self.current_cohort_frontbook_forecast['loan_cohort'].max().strftime(
                '%Y-%m') + ' (backbook)...')

            self.current_cohort_frontbook_forecast = self.generate_new_days(self.current_cohort_frontbook_forecast)

    def generate_frontbook_forecast(self):

        self.frontbook_forecast = pd.DataFrame()

        if self.fc_first_full_month_start_date <= self.fc_end_date:

            origination_fc = self.daily_origination_forecast[
                ['date', 'loan_cohort', 'first_cycle_type', 'country_code', 'currency_code', 'loan_type', 'creditor_id',
                 'principal', 'loan_count']].groupby([
                'date', 'loan_cohort', 'first_cycle_type', 'country_code', 'currency_code', 'loan_type', 'creditor_id'
            ]).sum().reset_index()

            cycle_type_list = origination_fc['first_cycle_type'].drop_duplicates().to_list()
            dates = self.origination_output_template['date'].drop_duplicates().to_list()
            cohorts = origination_fc['loan_cohort'].drop_duplicates().to_list()
            loan_types = origination_fc['loan_type'].drop_duplicates().to_list()
            countries = origination_fc['country_code'].drop_duplicates().to_list()
            country_currency_pairs = origination_fc[['country_code', 'currency_code']].drop_duplicates()
            country_creditor_pairs = origination_fc[['country_code', 'creditor_id']].drop_duplicates()

            first_month_cohort_fc = pd.DataFrame(
                list(itertools.product(dates, cohorts, cycle_type_list, countries, loan_types)),
                columns=['date', 'loan_cohort', 'first_cycle_type', 'country_code',
                         'loan_type']).drop_duplicates().merge(
                country_currency_pairs,
                how='left',
                on=['country_code']
            ).merge(
                country_creditor_pairs,
                how='left',
                on=['country_code']
            ).merge(
                origination_fc,
                how='left',
                on=['date', 'loan_cohort', 'first_cycle_type', 'loan_type', 'country_code', 'currency_code',
                    'creditor_id']
            )

            first_month_cohort_fc = self.generate_date_and_age_attributes(first_month_cohort_fc, 'date', 'loan_cohort')
            first_month_cohort_fc = first_month_cohort_fc[first_month_cohort_fc['age_bucket'] == '0M']
            first_month_cohort_fc = first_month_cohort_fc[
                first_month_cohort_fc['date'] >= first_month_cohort_fc['loan_cohort']]
            first_month_cohort_fc['principal'] = first_month_cohort_fc['principal'].fillna(0)
            first_month_cohort_fc['loan_count'] = first_month_cohort_fc['loan_count'].fillna(0)
            first_month_cohort_fc = first_month_cohort_fc.groupby([
                'date', 'loan_cohort', 'first_cycle_type', 'loan_type', 'creditor_id', 'country_code', 'currency_code'
            ]).sum().sort_values(by='date').groupby(
                ['loan_cohort', 'first_cycle_type', 'loan_type', 'creditor_id', 'country_code',
                 'currency_code']).cumsum().reset_index()
            first_month_cohort_fc['principal'] = first_month_cohort_fc.groupby(
                ['loan_cohort', 'first_cycle_type', 'loan_type', 'creditor_id', 'country_code', 'currency_code'])[
                'principal'].transform(lambda v: v.ffill())
            first_month_cohort_fc = first_month_cohort_fc.assign(day_id=first_month_cohort_fc['date'].dt.day).merge(
                self.balance_to_origination_ratios,
                how='left',
                on=['loan_type', 'country_code', 'currency_code', 'day_id']
            )
            first_month_cohort_fc.loc[first_month_cohort_fc['first_cycle_type'] == 'long', 'cum_balance_ratio'] = 1
            first_month_cohort_fc.loc[first_month_cohort_fc['first_cycle_type'] == 'long', 'cum_loan_ratio'] = 1
            first_month_cohort_fc['principal'] *= first_month_cohort_fc['cum_balance_ratio'].fillna(1)
            first_month_cohort_fc['loan_count'] *= first_month_cohort_fc['cum_loan_ratio'].fillna(1)
            first_month_cohort_fc = first_month_cohort_fc.drop(['cum_balance_ratio', 'cum_loan_ratio'], axis=1)
            first_month_cohort_fc = first_month_cohort_fc.rename(columns={'principal': 'principal_balance'})
            first_month_cohort_fc['balance_per_loan'] = first_month_cohort_fc['principal_balance'] / \
                                                        first_month_cohort_fc['loan_count']
            first_month_cohort_fc = self.generate_date_and_age_attributes(first_month_cohort_fc, 'date', 'loan_cohort')
            first_month_cohort_fc['data_source'] = 'front_book_forecast'
            first_month_cohort_fc = first_month_cohort_fc[first_month_cohort_fc['loan_count'] > 0]

            latest_balance = pd.concat([
                self.backbook_forecast,
                self.current_cohort_frontbook_forecast
            ])
            creditor_mix = self.generate_creditor_mix_of_latest_balance(
                latest_balance[latest_balance['date'] == self.fc_start_date]
            )

            for cohorts in first_month_cohort_fc['loan_cohort'].drop_duplicates().to_list():

                if cohorts == first_month_cohort_fc['loan_cohort'].min():
                    print('Generating cohort: ' + cohorts.strftime('%Y-%m') + ' (frontbook)...')
                else:
                    print('Generating cohort: ' + cohorts.strftime('%Y-%m') + '...')

                first_month_cohort_fc_i = first_month_cohort_fc[first_month_cohort_fc['loan_cohort'] == cohorts]
                first_month_cohort_fc_i_new_customers = first_month_cohort_fc_i[
                    first_month_cohort_fc_i['creditor_id'] != 'creditor_mix_of_current_portfolio']

                first_month_cohort_fc_i_existing_customers = first_month_cohort_fc_i[
                    first_month_cohort_fc_i['creditor_id'] == 'creditor_mix_of_current_portfolio'].merge(
                    creditor_mix,
                    how='left',
                    on=['country_code'],
                    suffixes=('_old', '')
                )
                first_month_cohort_fc_i_existing_customers['loan_count'] *= first_month_cohort_fc_i_existing_customers[
                    'creditor_share']
                first_month_cohort_fc_i_existing_customers['principal_balance'] *= \
                    first_month_cohort_fc_i_existing_customers['creditor_share']
                first_month_cohort_fc_i_existing_customers['balance_per_loan'] = \
                    first_month_cohort_fc_i_existing_customers['principal_balance'] / \
                    first_month_cohort_fc_i_existing_customers['loan_count']
                first_month_cohort_fc_i_existing_customers = first_month_cohort_fc_i_existing_customers.drop(
                    ['creditor_share', 'creditor_id_old'], axis=1)

                first_month_cohort_fc_i = pd.concat([
                    first_month_cohort_fc_i_existing_customers,
                    first_month_cohort_fc_i_new_customers
                ])

                self.frontbook_forecast = pd.concat([
                    self.frontbook_forecast,
                    self.generate_new_days(first_month_cohort_fc_i[first_month_cohort_fc_i['loan_cohort'] == cohorts],
                                           'front_book_forecast')
                ])

                latest_balance = pd.concat([
                    self.backbook_forecast,
                    self.current_cohort_frontbook_forecast,
                    self.frontbook_forecast
                ])

                cohort_i_origination = self.daily_origination_forecast[
                    (self.daily_origination_forecast['loan_cohort'] == cohorts) &
                    (self.daily_origination_forecast['origination_type'] == 'existing_customer')
                    ].drop(['creditor_id'], axis=1).merge(
                    creditor_mix,
                    how='left',
                    on=['country_code']
                )
                cohort_i_origination['principal'] *= cohort_i_origination['creditor_share']
                cohort_i_origination['loan_count'] *= cohort_i_origination['creditor_share']
                cohort_i_origination = cohort_i_origination.drop(['creditor_share'], axis=1)

                self.daily_origination_forecast = pd.concat([

                    self.daily_origination_forecast[self.daily_origination_forecast['loan_cohort'] != cohorts],

                    self.daily_origination_forecast[
                        (self.daily_origination_forecast['loan_cohort'] == cohorts) &
                        (self.daily_origination_forecast['origination_type'] == 'new_customer')
                        ],

                    cohort_i_origination

                ])

                creditor_mix = self.generate_creditor_mix_of_latest_balance(
                    latest_balance[latest_balance['date'] < cohorts + pd.DateOffset(months=1)])

    def generate_creditor_mix_of_latest_balance(self, latest_balance_data):

        creditor_mix = latest_balance_data[latest_balance_data['date'] == latest_balance_data['date'].max()]
        creditor_mix = creditor_mix[['country_code', 'creditor_id', 'loan_count']].groupby(
            ['country_code', 'creditor_id']).sum().reset_index()
        creditor_mix = creditor_mix.merge(
            creditor_mix.groupby(['country_code']).sum().reset_index(),
            how='left',
            on=['country_code'],
            suffixes=('', '_country_tot')
        )
        creditor_mix['creditor_share'] = creditor_mix['loan_count'] / creditor_mix['loan_count_country_tot']
        creditor_mix = creditor_mix.drop(['loan_count', 'loan_count_country_tot'], axis=1)

        return creditor_mix

    def generate_spv_forecasts(self):

        self.spv_forecasts = []

        for spv in self.spv_settings['creditor ID'].to_list():
            spv_i = AnyfinSPVModel(

                creditor_id=spv,
                IPD_reset_day=self.spv_settings[self.spv_settings['creditor ID'] == spv]['IPD reset day'].values[0],
                IPD_reset_payment_day=
                self.spv_settings[self.spv_settings['creditor ID'] == spv]['IPD reset payment day'].values[0],
                country_code=self.spv_settings[self.spv_settings['creditor ID'] == spv]['Country'].values[0],
                currency_code=self.spv_settings[self.spv_settings['creditor ID'] == spv]['Currency'].values[0],
                balance_as_of_latest_IPD_reset_date=
                self.spv_settings[self.spv_settings['creditor ID'] == spv]['Balance at latest IPD reset date'].values[
                    0],
                TA_start=
                self.spv_settings[self.spv_settings['creditor ID'] == spv]['Transaction account start balance'].values[
                    0],
                FA_start=
                self.spv_settings[self.spv_settings['creditor ID'] == spv]['Funding account start balance'].values[0],
                cumulative_losses=
                self.spv_settings[self.spv_settings['creditor ID'] == spv]['Cumulative losses'].values[0],
                loss_absorbing_lender=
                self.spv_settings[self.spv_settings['creditor ID'] == spv]['Loss absorbing lender'].values[0],
                lender_amounts_at_date=self.spv_lenders[self.spv_lenders['creditor ID'] == spv],
                draw_down_schedule=self.spv_draw_downs[self.spv_draw_downs['creditor ID'] == spv],
                portfolio_forecast=self
            )

            spv_i.generate_complete_spv_forecast()

            self.spv_forecasts.append(spv_i)

    def estimate_credit_indicator_shares(self):

        eligibility_type_list = self.raw_credit_risk_indicator_data['eligibility_type'].drop_duplicates().to_list()
        cpd_type_list = self.raw_credit_risk_indicator_data['cpd'].drop_duplicates().to_list()
        credit_score_list = self.raw_credit_risk_indicator_data['score_bucket'].drop_duplicates().to_list()
        holiday_list = self.raw_credit_risk_indicator_data['on_holiday'].drop_duplicates().to_list()
        age_list = self.raw_credit_risk_indicator_data['age_group'].drop_duplicates().to_list()
        day_id_list = range(1, 32)
        country_list = self.raw_credit_risk_indicator_data['country_code'].drop_duplicates().to_list()
        country_currency_pairs = self.raw_credit_risk_indicator_data[
            ['country_code', 'currency_code']].drop_duplicates()

        template = pd.DataFrame(
            list(itertools.product(country_list, eligibility_type_list, cpd_type_list, credit_score_list, holiday_list,
                                   age_list, day_id_list)),
            columns=['country_code', 'eligibility_type', 'cpd', 'score_bucket', 'on_holiday', 'age_group',
                     'day_id']).drop_duplicates().merge(
            country_currency_pairs,
            how='left',
            on=['country_code']
        )

        self.credit_risk_indicator_shares = template.merge(
            self.raw_credit_risk_indicator_data,
            how='left',
            on=['country_code', 'currency_code', 'eligibility_type', 'cpd', 'score_bucket', 'on_holiday', 'age_group',
                'day_id']
        ).fillna(0)

        self.credit_risk_indicator_shares = self.credit_risk_indicator_shares.merge(
            self.credit_risk_indicator_shares[
                ['country_code', 'currency_code', 'age_group', 'day_id', 'principal_balance', 'loan_count']].groupby(
                ['country_code', 'currency_code', 'age_group', 'day_id']).sum().reset_index(),
            how='left',
            on=['country_code', 'currency_code', 'age_group', 'day_id'],
            suffixes=('', '_tot')
        )

        self.credit_risk_indicator_shares['principal_share'] = (
                self.credit_risk_indicator_shares['principal_balance'] / self.credit_risk_indicator_shares[
            'principal_balance_tot']).replace([-np.inf, np.inf], np.nan).fillna(0)
        self.credit_risk_indicator_shares['loan_share'] = (
                self.credit_risk_indicator_shares['loan_count'] / self.credit_risk_indicator_shares[
            'loan_count_tot']).replace([-np.inf, np.inf], np.nan).fillna(0)
        self.credit_risk_indicator_shares = self.credit_risk_indicator_shares.drop(
            ['principal_balance', 'principal_balance_tot', 'loan_count', 'loan_count_tot'], axis=1)

    def generate_credit_risk_indicator_forecast(self):

        credit_risk_indicator_forecast = pd.concat([

            self.current_cohort_frontbook_forecast.drop(['balance_per_loan'], axis=1, errors='ignore'),
            self.frontbook_forecast.drop(['balance_per_loan'], axis=1, errors='ignore'),
            self.backbook_forecast.drop(['balance_per_loan'], axis=1)

        ])

        credit_risk_indicator_forecast['cohort_age_months'] = 12 * (
                credit_risk_indicator_forecast['date'].dt.year - credit_risk_indicator_forecast[
            'loan_cohort'].dt.year) + (credit_risk_indicator_forecast['date'].dt.month -
                                       credit_risk_indicator_forecast['loan_cohort'].dt.month)
        bins = [-np.inf, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, np.inf]
        labels = ['0M', '1M', '2M', '3M', '4M', '5M', '6M', '7M', '8M', '9M', '10M', '11M', '12M', '>12M']
        credit_risk_indicator_forecast['age_group'] = pd.cut(credit_risk_indicator_forecast['cohort_age_months'],
                                                             bins=bins, labels=labels)
        credit_risk_indicator_forecast['day_id'] = credit_risk_indicator_forecast['date'].dt.day
        credit_risk_indicator_forecast = credit_risk_indicator_forecast.merge(
            self.credit_risk_indicator_shares,
            how='left',
            on=['country_code', 'currency_code', 'age_group', 'day_id']
        )

        credit_risk_indicator_forecast['principal_balance'] *= credit_risk_indicator_forecast['principal_share']
        credit_risk_indicator_forecast['loan_count'] *= credit_risk_indicator_forecast['loan_share']

        ############################################################################################################################
        ####  CPD and eligibility type adjustments in this section  ################################################################
        ############################################################################################################################

        credit_risk_indicator_forecast = credit_risk_indicator_forecast.merge(
            self.adjustment_forecast[['date', 'country_code', 'backbook_cpd_adjustment', 'frontbook_cpd_adjustment']],
            how='left',
            on=['date', 'country_code']
        ).assign(cpd_adj=1)

        credit_risk_indicator_forecast.loc[
            (credit_risk_indicator_forecast['data_source'] == 'backbook_forecast') &
            (credit_risk_indicator_forecast['cpd'] != '0 cpd'),
            'cpd_adj'
        ] = credit_risk_indicator_forecast['backbook_cpd_adjustment']

        credit_risk_indicator_forecast.loc[
            (
                (credit_risk_indicator_forecast['data_source'] == 'front_book_forecast')
            ) &
            (credit_risk_indicator_forecast['cpd'] != '0 cpd'),
            'cpd_adj'
        ] = credit_risk_indicator_forecast['frontbook_cpd_adjustment']
        credit_risk_indicator_forecast['loan_adj'] = credit_risk_indicator_forecast['loan_count'] * (
                credit_risk_indicator_forecast['cpd_adj'] - 1)
        credit_risk_indicator_forecast['principal_balance_adj'] = credit_risk_indicator_forecast[
                                                                      'principal_balance'] * (
                                                                          credit_risk_indicator_forecast[
                                                                              'cpd_adj'] - 1)

        credit_risk_indicator_forecast = pd.concat([

            credit_risk_indicator_forecast.assign(
                loan_count=credit_risk_indicator_forecast['loan_count'] + credit_risk_indicator_forecast['loan_adj'],
                principal_balance=credit_risk_indicator_forecast['principal_balance'] + credit_risk_indicator_forecast[
                    'principal_balance_adj'],
            )[['date', 'loan_cohort', 'data_source', 'loan_type', 'country_code', 'currency_code', 'eligibility_type',
               'creditor_id', 'cpd', 'score_bucket', 'on_holiday', 'loan_count', 'principal_balance']]
            ,
            credit_risk_indicator_forecast.assign(
                loan_count=-credit_risk_indicator_forecast['loan_adj'],
                principal_balance=-credit_risk_indicator_forecast['principal_balance_adj'],
                cpd='0 cpd',
            )[['date', 'loan_cohort', 'data_source', 'loan_type', 'country_code', 'currency_code', 'eligibility_type',
               'creditor_id', 'cpd', 'score_bucket', 'on_holiday', 'loan_count', 'principal_balance']]

        ]).groupby(
            ['date', 'loan_cohort', 'data_source', 'loan_type', 'country_code', 'currency_code', 'eligibility_type',
             'creditor_id', 'cpd', 'score_bucket', 'on_holiday']).sum().reset_index()

        credit_risk_indicator_forecast = credit_risk_indicator_forecast.merge(
            self.adjustment_forecast[['date', 'country_code', 'backbook_cpd_adjustment', 'frontbook_cpd_adjustment']],
            how='left',
            on=['date', 'country_code']
        ).assign(cpd_adj=1)

        credit_risk_indicator_forecast.loc[
            (credit_risk_indicator_forecast['data_source'] == 'backbook_forecast') &
            (credit_risk_indicator_forecast['eligibility_type'] != 'other'),
            'cpd_adj'
        ] = credit_risk_indicator_forecast['backbook_cpd_adjustment']

        credit_risk_indicator_forecast.loc[
            (
                (credit_risk_indicator_forecast['data_source'] == 'front_book_forecast')
            ) &
            (credit_risk_indicator_forecast['eligibility_type'] != 'other'),
            'cpd_adj'
        ] = credit_risk_indicator_forecast['frontbook_cpd_adjustment']

        credit_risk_indicator_forecast['loan_adj'] = credit_risk_indicator_forecast['loan_count'] * (
                credit_risk_indicator_forecast['cpd_adj'] - 1)
        credit_risk_indicator_forecast['principal_balance_adj'] = credit_risk_indicator_forecast[
                                                                      'principal_balance'] * (
                                                                          credit_risk_indicator_forecast[
                                                                              'cpd_adj'] - 1)

        credit_risk_indicator_forecast = pd.concat([

            credit_risk_indicator_forecast.assign(
                loan_count=credit_risk_indicator_forecast['loan_count'] + credit_risk_indicator_forecast['loan_adj'],
                principal_balance=credit_risk_indicator_forecast['principal_balance'] + credit_risk_indicator_forecast[
                    'principal_balance_adj'],
            )[['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code', 'eligibility_type', 'creditor_id',
               'cpd', 'score_bucket', 'on_holiday', 'loan_count', 'principal_balance']]
            ,
            credit_risk_indicator_forecast.assign(
                loan_count=-credit_risk_indicator_forecast['loan_adj'],
                principal_balance=-credit_risk_indicator_forecast['principal_balance_adj'],
                eligibility_type='Other',
            )[['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code', 'eligibility_type', 'creditor_id',
               'cpd', 'score_bucket', 'on_holiday', 'loan_count', 'principal_balance']]

        ]).groupby(
            ['date', 'loan_cohort', 'loan_type', 'country_code', 'currency_code', 'eligibility_type', 'creditor_id',
             'cpd', 'score_bucket', 'on_holiday']).sum().reset_index()

        ############################################################################################################################
        ############################################################################################################################
        ############################################################################################################################

        self.credit_risk_indicator_output = pd.concat([

            credit_risk_indicator_forecast[
                ['date', 'country_code', 'currency_code', 'creditor_id', 'eligibility_type', 'cpd', 'on_holiday',
                 'score_bucket', 'principal_balance', 'loan_count']].groupby(
                ['date', 'country_code', 'currency_code', 'creditor_id', 'eligibility_type', 'cpd', 'on_holiday',
                 'score_bucket']
            ).sum().reset_index(),

            self.raw_credit_risk_indicator_data_recent[
                self.raw_credit_risk_indicator_data_recent['date'] < self.fc_start_date][
                ['date', 'country_code', 'currency_code', 'creditor_id', 'eligibility_type', 'cpd', 'on_holiday',
                 'score_bucket', 'principal_balance', 'loan_count']].groupby(
                ['date', 'country_code', 'currency_code', 'creditor_id', 'eligibility_type', 'cpd', 'on_holiday',
                 'score_bucket']
            ).sum().reset_index()

        ])

    def generate_daily_cashflows(self):

        balance_output = pd.concat([

            self.current_cohort_frontbook_forecast.drop(['balance_per_loan'], axis=1, errors='ignore'),
            self.frontbook_forecast.drop(['balance_per_loan'], axis=1, errors='ignore'),
            self.backbook_forecast.drop(['balance_per_loan'], axis=1),
            self.raw_balance_data[
                (self.raw_balance_data['date'] < self.fc_start_date) &
                (self.raw_balance_data['date'] >= self.fc_start_date + pd.DateOffset(years=-1))][
                ['date', 'loan_cohort', 'country_code', 'loan_type', 'creditor_id', 'currency_code',
                 'principal_balance', 'loan_count']
            ].groupby(
                ['date', 'loan_cohort', 'country_code', 'loan_type', 'creditor_id', 'currency_code']
            ).sum().reset_index().assign(data_source='actual')
        ])[['date', 'country_code', 'currency_code', 'creditor_id', 'principal_balance']].groupby(
            ['date', 'country_code', 'currency_code', 'creditor_id']).sum().reset_index()

        origination_output = pd.concat([

            self.raw_origination_data[
                (self.raw_origination_data['date'] < self.fc_start_date) &
                (self.raw_origination_data['date'] >= self.fc_start_date + pd.DateOffset(years=-1))
                ].assign(data_source='actual'),
            self.daily_origination_forecast

        ])[['date', 'country_code', 'currency_code', 'creditor_id', 'principal']].groupby(
            ['date', 'country_code', 'currency_code', 'creditor_id']).sum().reset_index()

        date_list = balance_output['date'].drop_duplicates().to_list()
        country_list = balance_output['country_code'].drop_duplicates().to_list()
        creditor_list = list(set(balance_output['creditor_id'].drop_duplicates().to_list() + origination_output[
            'creditor_id'].drop_duplicates().to_list()))
        country_currency_pairs = balance_output[['country_code', 'currency_code']].drop_duplicates()

        template = pd.DataFrame(
            list(itertools.product(date_list, country_list, creditor_list)),
            columns=['date', 'country_code', 'creditor_id']).drop_duplicates().merge(
            country_currency_pairs,
            how='left',
            on=['country_code']
        )

        self.daily_cashflows = template.merge(
            balance_output,
            how='left',
            on=['date', 'country_code', 'currency_code', 'creditor_id']
        ).merge(
            origination_output.rename(columns={'principal': 'origination'}),
            how='left',
            on=['date', 'country_code', 'currency_code', 'creditor_id']
        )

        self.daily_cashflows = self.daily_cashflows.merge(
            self.daily_cashflows.assign(date=self.daily_cashflows['date'] + pd.DateOffset(days=1)),
            how='left',
            on=['date', 'country_code', 'currency_code', 'creditor_id'],
            suffixes=('', '_yesterday')
        )

        self.daily_cashflows['repayments'] = self.daily_cashflows['principal_balance'].fillna(0) - self.daily_cashflows[
            'principal_balance_yesterday'].fillna(0) - self.daily_cashflows['origination'].fillna(0)
        self.daily_cashflows = self.daily_cashflows.drop(['principal_balance_yesterday', 'origination_yesterday'],
                                                         axis=1)
        self.daily_cashflows['repayments'] *= -1
        self.daily_cashflows['origination'] *= -1

    def daily_funding_rate(self, balance, funding_cost_scheme):

        remaining_balance = balance
        balance_cost_allocated = 0
        funding_cost = 0

        for balance_intervals in funding_cost_scheme.sort_values(by=['Up to amount'])['Up to amount']:
            funding_rate = \
                funding_cost_scheme[funding_cost_scheme['Up to amount'] == balance_intervals]['funding_rate'].values[0]
            funding_cost += funding_rate * min(remaining_balance, balance_intervals - balance_cost_allocated)
            remaining_balance = remaining_balance - min(remaining_balance, balance_intervals - balance_cost_allocated)
            balance_cost_allocated = balance - remaining_balance

        return funding_cost / balance

    def generate_funding_cost_forecast(self):

        print('Generating funding cost forecast...')

        self.funding_cost_forecast = pd.concat([
            self.current_cohort_frontbook_forecast.drop(['balance_per_loan'], axis=1, errors='ignore'),
            self.frontbook_forecast.drop(['balance_per_loan'], axis=1, errors='ignore'),
            self.backbook_forecast.drop(['balance_per_loan'], axis=1)
        ])[['date', 'creditor_id', 'principal_balance']].groupby(['date', 'creditor_id']).sum().reset_index().assign(
            avg_funding_cost_rate=0
        )

        funding_costs_scheme = self.funding_costs_assumptions
        funding_costs_scheme['funding_rate'] = funding_costs_scheme['Reference rate value'] + funding_costs_scheme[
            'Spread value']
        funding_costs_scheme = funding_costs_scheme[['Creditor ID', 'Apply from date', 'funding_rate', 'Up to amount']]

        for creditor in funding_costs_scheme['Creditor ID'].drop_duplicates().to_list():

            for start_dates in \
                    funding_costs_scheme[funding_costs_scheme['Creditor ID'] == creditor].sort_values(
                        by=['Apply from date'])[
                        'Apply from date'].drop_duplicates().to_list():

                applicable_funding_cost_scheme = funding_costs_scheme[
                    (funding_costs_scheme['Creditor ID'] == creditor) &
                    (funding_costs_scheme['Apply from date'] == start_dates)
                    ]

                applicable_period = self.funding_cost_forecast[
                    (self.funding_cost_forecast['creditor_id'] == creditor) &
                    (self.funding_cost_forecast['date'] >= start_dates)
                    ]

                if applicable_period.empty:
                    continue

                applicable_period['avg_funding_cost_rate'] = applicable_period.apply(
                    lambda x: self.daily_funding_rate(x['principal_balance'], applicable_funding_cost_scheme), axis=1)
                applicable_period = applicable_period.drop(['principal_balance'], axis=1)

                self.funding_cost_forecast = self.funding_cost_forecast.merge(
                    applicable_period,
                    how='left',
                    on=['date', 'creditor_id'],
                    suffixes=('', '_assumed')
                )
                self.funding_cost_forecast['avg_funding_cost_rate'] = self.funding_cost_forecast[
                    'avg_funding_cost_rate_assumed'].combine_first(self.funding_cost_forecast['avg_funding_cost_rate'])
                self.funding_cost_forecast = self.funding_cost_forecast.drop(['avg_funding_cost_rate_assumed'], axis=1)

        self.funding_cost_forecast = self.funding_cost_forecast.drop(['principal_balance'], axis=1)

    def add_interest_income(self):

        print('Generating interest income forecast...')

        interest_at_start = self.raw_balance_data[
            self.raw_balance_data['date'] == self.raw_balance_data['date'].max()
            ][['creditor_id', 'loan_type', 'country_code', 'currency_code', 'loan_cohort', 'avg_interest',
               'principal_balance']]
        interest_at_start['interest_amount'] = interest_at_start['principal_balance'] * interest_at_start[
            'avg_interest']
        interest_at_start = interest_at_start.groupby([
            'creditor_id', 'loan_type', 'country_code', 'currency_code', 'loan_cohort'
        ]).sum()
        interest_at_start['avg_interest'] = interest_at_start['interest_amount'] / interest_at_start[
            'principal_balance']
        interest_at_start = interest_at_start.drop(['interest_amount', 'principal_balance'], axis=1)

        self.backbook_forecast = self.backbook_forecast.merge(
            interest_at_start,
            how='left',
            on=['creditor_id', 'loan_type', 'country_code', 'currency_code', 'loan_cohort']
        )

        if self.current_cohort_frontbook_forecast.empty == False:
            self.current_cohort_frontbook_forecast = self.current_cohort_frontbook_forecast.merge(
                interest_at_start,
                how='left',
                on=['creditor_id', 'loan_type', 'country_code', 'currency_code', 'loan_cohort']
            )
            self.current_cohort_frontbook_forecast.loc[
                self.current_cohort_frontbook_forecast['data_source'] != 'backbook_forecast', 'avg_interest'] = 0

        self.frontbook_forecast['avg_interest'] = 0

        for country in self.revenue_assumptions['Country'].drop_duplicates().to_list():

            for dates in \
                    self.revenue_assumptions[self.revenue_assumptions['Country'] == country].sort_values(
                        by='Apply from date')[
                        'Apply from date']:

                applicable_backbook_shift = self.revenue_assumptions[
                    (self.revenue_assumptions['Country'] == country) &
                    (self.revenue_assumptions['Apply from date'] == dates)
                    ]['Backbook shift'].values[0]

                applicable_frontbook_rate = self.revenue_assumptions[
                    (self.revenue_assumptions['Country'] == country) &
                    (self.revenue_assumptions['Apply from date'] == dates)
                    ]['Front book total rate'].values[0]

                self.backbook_forecast.loc[
                    (self.backbook_forecast['country_code'] == country) &
                    (self.backbook_forecast['date'] >= dates)
                    ,
                    'avg_interest'
                ] += applicable_backbook_shift

                if self.current_cohort_frontbook_forecast.empty == False:
                    self.current_cohort_frontbook_forecast.loc[
                        (self.current_cohort_frontbook_forecast['country_code'] == country) &
                        (self.current_cohort_frontbook_forecast['date'] >= dates) &
                        (self.current_cohort_frontbook_forecast['data_source'] == 'front_book_current_cohort')
                        ,
                        'avg_interest'
                    ] = applicable_frontbook_rate

                    self.current_cohort_frontbook_forecast.loc[
                        (self.current_cohort_frontbook_forecast['country_code'] == country) &
                        (self.current_cohort_frontbook_forecast['date'] >= dates) &
                        (self.current_cohort_frontbook_forecast['data_source'] == 'backbook_forecast')
                        ,
                        'avg_interest'
                    ] += applicable_backbook_shift

                self.frontbook_forecast.loc[
                    (self.frontbook_forecast['country_code'] == country) &
                    (self.frontbook_forecast['date'] >= dates)
                    ,
                    'avg_interest'
                ] = applicable_frontbook_rate

        self.backbook_forecast['interest_income'] = self.backbook_forecast['avg_interest'] * self.backbook_forecast[
            'principal_balance'] / 365

        if self.current_cohort_frontbook_forecast.empty == False:
            self.current_cohort_frontbook_forecast['interest_income'] = self.current_cohort_frontbook_forecast[
                                                                            'avg_interest'] * \
                                                                        self.current_cohort_frontbook_forecast[
                                                                            'principal_balance'] / 365

        self.frontbook_forecast['interest_income'] = self.frontbook_forecast['avg_interest'] * self.frontbook_forecast[
            'principal_balance'] / 365

    def generate_customer_metric_forecast(self):

        customer_ratios = self.raw_customer_metric_data[
            self.raw_customer_metric_data['date'] == self.raw_customer_metric_data['date'].max()].groupby(
            ['country_code', 'currency_code', 'debt_group']).sum().reset_index()
        customer_ratios['loans_per_customer'] = customer_ratios['loan_count'] / customer_ratios['customer_count']
        customer_ratios = customer_ratios.drop(['principal_balance', 'customer_count', 'loan_count'], axis=1)

        customer_shares = self.raw_customer_metric_data[
            ['date', 'country_code', 'currency_code', 'debt_group', 'loan_count', 'principal_balance']].groupby([
            'date', 'country_code', 'currency_code', 'debt_group'
        ]).sum().reset_index().merge(

            self.raw_customer_metric_data[
                ['date', 'country_code', 'currency_code', 'principal_balance', 'loan_count']].groupby([
                'date', 'country_code', 'currency_code'
            ]).sum().reset_index(),

            how='left',
            on=['date', 'country_code', 'currency_code'],
            suffixes=('', '_tot')
        )
        customer_shares['balance_share'] = customer_shares['principal_balance'] / customer_shares[
            'principal_balance_tot']
        customer_shares['loan_share'] = customer_shares['loan_count'] / customer_shares['loan_count_tot']
        customer_shares = customer_shares.drop(
            ['principal_balance_tot', 'principal_balance', 'loan_count', 'loan_count_tot'], axis=1)

        customer_share_fc = customer_shares[customer_shares['date'] == customer_shares['date'].max()].drop(['date'],
                                                                                                           axis=1)

        self.customer_metric_forecast = pd.concat([
            self.current_cohort_frontbook_forecast.drop(['balance_per_loan'], axis=1, errors='ignore'),
            self.frontbook_forecast.drop(['balance_per_loan'], axis=1, errors='ignore'),
            self.backbook_forecast.drop(['balance_per_loan'], axis=1)
        ]).merge(
            self.funding_cost_forecast,
            how='left',
            on=['date', 'creditor_id']
        )[['date', 'country_code', 'currency_code', 'principal_balance', 'loan_count']].groupby(
            ['date', 'country_code', 'currency_code']).sum().reset_index().merge(
            customer_share_fc,
            how='left',
            on=['country_code', 'currency_code']
        )
        self.customer_metric_forecast['principal_balance'] *= self.customer_metric_forecast['balance_share']
        self.customer_metric_forecast['loan_count'] *= self.customer_metric_forecast['loan_share']

        self.customer_metric_forecast = self.customer_metric_forecast.drop(['balance_share', 'loan_share'], axis=1)
        self.customer_metric_forecast = self.customer_metric_forecast.merge(
            customer_ratios,
            how='left',
            on=['country_code', 'currency_code', 'debt_group']
        )
        self.customer_metric_forecast['customer_count'] = self.customer_metric_forecast['loan_count'] / \
                                                          self.customer_metric_forecast['loans_per_customer']
        self.customer_metric_forecast = self.customer_metric_forecast.drop(['loans_per_customer'], axis=1)

    def collect_and_export_final_output(self):

        # Collect and clean up forecast results

        balance_forecast = pd.concat([
            self.current_cohort_frontbook_forecast.drop(['balance_per_loan'], axis=1, errors='ignore'),
            self.frontbook_forecast.drop(['balance_per_loan'], axis=1, errors='ignore'),
            self.backbook_forecast.drop(['balance_per_loan'], axis=1)
        ]).merge(
            self.funding_cost_forecast,
            how='left',
            on=['date', 'creditor_id']
        )
        balance_forecast['funding_cost'] = balance_forecast['avg_funding_cost_rate'] * balance_forecast[
            'principal_balance'] / 360
        balance_forecast = balance_forecast.drop(['avg_funding_cost_rate'], axis=1)

        actual_balances = self.raw_balance_data.assign(
            interest_income_amount_temp=self.raw_balance_data['avg_interest'] * self.raw_balance_data[
                'principal_balance']
        )
        actual_balances = self.generate_date_and_age_attributes(
            actual_balances[
                (actual_balances['date'] < self.fc_start_date) &
                (actual_balances['date'] >= self.fc_start_date + pd.DateOffset(years=-1))][
                ['date', 'loan_cohort', 'first_cycle_type', 'country_code', 'loan_type', 'creditor_id', 'currency_code',
                 'principal_balance', 'loan_count', 'interest_income_amount_temp']
            ].groupby(
                ['date', 'loan_cohort', 'first_cycle_type', 'country_code', 'loan_type', 'creditor_id', 'currency_code']
            ).sum().reset_index().assign(
                data_source='actual'),
            'date',
            'loan_cohort'
        )
        actual_balances['avg_interest'] = actual_balances['interest_income_amount_temp'] / actual_balances[
            'principal_balance']
        actual_balances = actual_balances.drop(['interest_income_amount_temp'], axis=1)
        actual_balances['interest_income'] = actual_balances['avg_interest'] * actual_balances[
            'principal_balance'] / 365

        self.main_balance_output = pd.concat([balance_forecast, actual_balances])
        self.main_balance_output = self.main_balance_output.assign(
            principal_balance_SEK=self.main_balance_output.principal_balance,
            principal_balance_EUR=self.main_balance_output.principal_balance,
            principal_balance_USD=self.main_balance_output.principal_balance
        )
        self.main_balance_output.loc[
            self.main_balance_output['currency_code'] == 'EUR', 'principal_balance_SEK'] *= self.SEK_EUR_plan_rate
        self.main_balance_output.loc[self.main_balance_output['currency_code'] == 'SEK', 'principal_balance_SEK'] *= 1.0
        self.main_balance_output.loc[self.main_balance_output['currency_code'] == 'EUR', 'principal_balance_EUR'] *= 1.0
        self.main_balance_output.loc[
            self.main_balance_output['currency_code'] == 'SEK', 'principal_balance_EUR'] /= self.SEK_EUR_plan_rate
        self.main_balance_output.loc[
            self.main_balance_output['currency_code'] == 'EUR', 'principal_balance_USD'] *= self.EUR_USD_plan_rate
        self.main_balance_output.loc[
            self.main_balance_output['currency_code'] == 'SEK', 'principal_balance_USD'] *= self.SEK_USD_plan_rate
        self.main_balance_output = self.main_balance_output.assign(forecast_version_id=self.forecast_version_id)
        self.main_balance_output = self.main_balance_output[self.main_balance_output['date'] <= self.fc_end_date_output]
        self.main_balance_output['days_since'] = self.main_balance_output['date'] - self.main_balance_output[
            'loan_cohort']
        self.main_balance_output = self.main_balance_output.drop(['avg_interest'], axis=1)

        self.main_origination_output = self.generate_date_and_age_attributes(
            pd.concat([
                self.raw_origination_data[
                    (self.raw_origination_data['date'] < self.fc_start_date) &
                    (self.raw_origination_data['date'] >= self.fc_start_date + pd.DateOffset(years=-1))
                    ].assign(data_source='actual'),
                self.daily_origination_forecast
            ]),
            'date',
            'loan_cohort'
        ).assign(forecast_version_id=self.forecast_version_id)
        self.main_origination_output = self.main_origination_output[
            self.main_origination_output['date'] <= self.fc_end_date_output]

        self.credit_risk_indicator_output = self.credit_risk_indicator_output[
            self.credit_risk_indicator_output['date'] <= self.fc_end_date_output]

        self.customer_metric_output = pd.concat([

            self.raw_customer_metric_data[
                (self.raw_customer_metric_data['date'] >= self.fc_start_date + pd.DateOffset(years=-1)) &
                (self.raw_customer_metric_data['date'] < self.fc_start_date)
                ].assign(data_source='actual'),
            self.customer_metric_forecast.assign(data_source='forecast')

        ])

        # Export csv files

        self.main_origination_output.to_csv(self.data_storage_path + 'main_origination_output.csv', index=False)
        self.main_balance_output.to_csv(self.data_storage_path + 'main_balance_output.csv', index=False)
        self.credit_risk_indicator_output.to_csv(self.data_storage_path + 'credit_risk_indicators_output.csv',
                                                 index=False)
        self.daily_cashflows.to_csv(self.data_storage_path + 'portfolio_daily_cash_flows.csv', index=False)
        self.funding_cost_forecast.to_csv(self.data_storage_path + 'funding_cost_rates.csv', index=False)
        self.customer_metric_output.to_csv(self.data_storage_path + 'customer_metrics.csv', index=False)

        spv_balance_sheets = pd.DataFrame()
        spv_cashflow_data = pd.DataFrame()

        for spv in self.spv_forecasts:
            spv_cashflow_data = pd.concat([spv_cashflow_data, spv.daily_spv_cashflows])
            spv_balance_sheets = pd.concat([spv_balance_sheets, spv.balance_sheet])

        spv_cashflow_data.to_csv(self.data_storage_path + 'spv_cash_flows.csv', index=False)
        spv_balance_sheets.to_csv(self.data_storage_path + 'spv_balance_sheets.csv', index=False)

    def generate_complete_forecast(self):

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

        self.generate_pre_current_cohort_backbook_forecast()
        self.generate_current_cohort_backbook_forecast()
        self.generate_frontbook_forecast()

        # Additional metrics forecast

        self.generate_credit_risk_indicator_forecast()
        self.generate_daily_cashflows()
        self.generate_funding_cost_forecast()
        self.add_interest_income()
        self.generate_customer_metric_forecast()

        # Generate SPV forecasts

        self.generate_spv_forecasts()

        # Collect and export results

        self.collect_and_export_final_output()

    def generate_remaining_term_decline_rates(self):

        # NOT USED AT THE MOMENT - AWAITING EXPECTED END DATE DATA

        model_estimation_data = self.raw_balance_data.copy()
        model_estimation_data['avg_remaining_term'] = model_estimation_data['avg_remaining_term'] * \
                                                      model_estimation_data['principal_balance']
        model_estimation_data = model_estimation_data[[
            'date', 'country_code', 'loan_type', 'loan_cohort', 'avg_remaining_term', 'principal_balance'
        ]].groupby([
            'date', 'country_code', 'loan_type', 'loan_cohort'
        ]).sum().reset_index()
        model_estimation_data['avg_remaining_term'] = model_estimation_data['avg_remaining_term'] / \
                                                      model_estimation_data['principal_balance']
        model_estimation_data = model_estimation_data.merge(
            model_estimation_data.assign(date=model_estimation_data['date'] + pd.DateOffset(days=1)),
            how='left',
            on=['date', 'country_code', 'loan_type', 'loan_cohort'],
            suffixes=('', '_prev')
        )
        model_estimation_data = self.generate_date_and_age_attributes(model_estimation_data, 'date', 'loan_cohort')
        model_estimation_data = model_estimation_data[

            (model_estimation_data['age_bucket'] != 0) &
            (model_estimation_data['avg_remaining_term_prev'] > 0) &
            (model_estimation_data['date'] >= self.fc_start_date + pd.DateOffset(years=-1))
            ]
        model_estimation_data['avg_remaining_term_decline'] = (model_estimation_data['avg_remaining_term'] -
                                                               model_estimation_data['avg_remaining_term_prev']) * \
                                                              model_estimation_data['principal_balance']
        model_estimation_data['avg_remaining_term'] = model_estimation_data['avg_remaining_term'] * \
                                                      model_estimation_data['principal_balance']
        model_estimation_data = model_estimation_data[
            ['country_code', 'loan_type', 'age_bucket', 'avg_remaining_term', 'principal_balance',
             'avg_remaining_term_decline']].groupby(['country_code', 'loan_type', 'age_bucket']).sum().reset_index()
        model_estimation_data['avg_remaining_term'] /= model_estimation_data['principal_balance']
        model_estimation_data['avg_remaining_term_decline'] /= model_estimation_data['principal_balance']
        model_estimation_data['avg_remaining_term_decline'] = model_estimation_data[
            'avg_remaining_term_decline'].fillna(0)

        model_estimation_data = pd.concat([
            model_estimation_data[model_estimation_data['age_bucket'] == '1M'].assign(age_bucket='1M_short'),
            model_estimation_data[model_estimation_data['age_bucket'] == '1M'].assign(age_bucket='1M_long'),
            model_estimation_data[model_estimation_data['age_bucket'] != '1M']
        ])

        self.term_decline_forecast = self.balance_output_template.merge(
            model_estimation_data.drop(['avg_remaining_term', 'principal_balance'], axis=1),
            how='left',
            on=['country_code', 'loan_type', 'age_bucket']
        )
        self.term_decline_forecast['avg_remaining_term_decline'] = self.term_decline_forecast[
            'avg_remaining_term_decline'].fillna(0)

    def generate_remaining_term_forecast(self):

        # NOT USED AT THE MOMENT - AWAITING EXPECTED END DATE DATA

        original_terms = self.raw_balance_data.copy()
        original_terms['avg_original_term'] *= original_terms['principal_balance']
        original_terms = original_terms[
            (self.raw_balance_data['months_since'] == 0) &
            (self.raw_balance_data['date'] >= self.fc_start_date + pd.DateOffset(years=-1))
            ][['country_code', 'loan_type', 'principal_balance', 'avg_original_term']].groupby(
            ['country_code', 'loan_type']).sum().reset_index()
        original_terms['avg_original_term'] /= original_terms['principal_balance']

        starting_point = self.raw_balance_data[self.raw_balance_data['date'] < self.fc_start_date]
        starting_point = starting_point[starting_point['date'] == starting_point['date'].max()]
        starting_point['avg_remaining_term'] *= starting_point['principal_balance']
        starting_point['avg_original_term'] *= starting_point['principal_balance']
        starting_point = starting_point[
            ['date', 'country_code', 'loan_type', 'loan_cohort', 'principal_balance', 'avg_remaining_term',
             'avg_original_term']].groupby(['date', 'country_code', 'loan_type', 'loan_cohort']).sum().reset_index()
        starting_point['avg_remaining_term'] /= starting_point['principal_balance']
        starting_point['avg_original_term'] /= starting_point['principal_balance']
