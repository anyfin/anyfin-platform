import itertools
import pandas as pd
import holidays


class AnyfinSPVModel:

    def __init__(

            self,
            creditor_id,
            IPD_reset_day,
            IPD_reset_payment_day,
            country_code,
            currency_code,
            balance_as_of_latest_IPD_reset_date,
            TA_start,
            FA_start,
            cumulative_losses,
            loss_absorbing_lender,
            lender_amounts_at_date,
            draw_down_schedule,
            portfolio_forecast

    ):

        self.creditor_id = creditor_id
        self.IPD_reset_day = IPD_reset_day
        self.IPD_reset_payment_day = IPD_reset_payment_day
        self.country_code = country_code
        self.currency_code = currency_code
        self.balance_as_of_latest_IPD_reset_date = balance_as_of_latest_IPD_reset_date
        self.TA_start = TA_start
        self.FA_start = FA_start
        self.cumulative_losses = cumulative_losses
        self.loss_absorbing_lender = loss_absorbing_lender
        self.lender_amounts_at_date = lender_amounts_at_date
        self.draw_down_schedule = draw_down_schedule
        self.portfolio_forecast = portfolio_forecast

    def generate_output_template(self):

        self.spv_output_template = self.portfolio_forecast.balance_output_template[['date']].drop_duplicates().assign(
            country_code=self.country_code,
            currency_code=self.currency_code,
            creditor_id=self.creditor_id
        )

    def return_nearest_business_day(self, date, preceeding=False):

        if preceeding == False:

            days_value = 1

        else:

            days_value = -1

        if self.portfolio_forecast.date_is_holiday(date) == False and date.weekday() < 5:

            return date

        else:

            return self.return_nearest_business_day(date + pd.DateOffset(days=days_value), preceeding)

    def generate_spv_cashflows(self):

        print('Generating ' + self.creditor_id + ' cashflows...')

        self.daily_spv_cashflows = self.portfolio_forecast.daily_cashflows.copy()[
            (self.portfolio_forecast.daily_cashflows['creditor_id'] == self.creditor_id) &
            (self.portfolio_forecast.daily_cashflows['country_code'] == self.country_code)
            ].fillna(0)
        self.daily_spv_cashflows['date'] = self.daily_spv_cashflows.apply(
            lambda x: self.return_nearest_business_day(x.date + pd.DateOffset(days=1)), axis=1)
        self.daily_spv_cashflows = self.daily_spv_cashflows.groupby(
            ['date', 'country_code', 'creditor_id', 'currency_code']).sum().reset_index()

        self.daily_spv_cashflows = self.spv_output_template.merge(
            self.daily_spv_cashflows,
            how='left',
            on=['date', 'country_code', 'currency_code', 'creditor_id']

        ).fillna(0).drop(['principal_balance'], axis=1)

    def generate_TA_cash_flows(self):

        print('Generating ' + self.creditor_id + ' transaction account balances...')

        repayments = self.daily_spv_cashflows[

            (self.daily_spv_cashflows['creditor_id'] == self.creditor_id) &
            (self.daily_spv_cashflows['country_code'] == self.country_code) &
            (self.daily_spv_cashflows['currency_code'] == self.currency_code)

            ][['date', 'country_code', 'currency_code', 'creditor_id', 'repayments']].groupby(
            ['date', 'country_code', 'currency_code', 'creditor_id']).sum().reset_index()

        self.TA_cash_flows = self.spv_output_template.copy()

        nearest_IPD_reset_paymet_date = None

        for dates in self.TA_cash_flows.sort_values(by='date')['date'].drop_duplicates().to_list():

            if dates.day == self.IPD_reset_payment_day:
                nearest_IPD_reset_paymet_date = self.return_nearest_business_day(dates)

            repayment_amount_i = repayments[repayments['date'] == dates][['repayments']].values[0]

            if dates == self.TA_cash_flows['date'].min():

                account_balance_yesterday = self.TA_start
                account_balance_as_of_last_IPD_reset_date = self.balance_as_of_latest_IPD_reset_date

            else:

                account_balance_yesterday = \
                    self.TA_cash_flows[self.TA_cash_flows['date'] == dates + pd.DateOffset(days=-1)][
                        'account_balance'].values

            if dates == nearest_IPD_reset_paymet_date:

                IPD_reset_payment = -account_balance_as_of_last_IPD_reset_date

            else:

                IPD_reset_payment = 0

            account_balance = account_balance_yesterday + repayment_amount_i + IPD_reset_payment

            if dates.day == self.IPD_reset_day:
                account_balance_as_of_last_IPD_reset_date = account_balance

            self.TA_cash_flows.loc[self.TA_cash_flows['date'] == dates, 'account_balance'] = account_balance
            self.TA_cash_flows.loc[self.TA_cash_flows[
                                       'date'] == dates, 'account_balance_last_IPD_date'] = account_balance_as_of_last_IPD_reset_date
            self.TA_cash_flows.loc[self.TA_cash_flows['date'] == dates, 'repayment'] = +repayment_amount_i
            self.TA_cash_flows.loc[self.TA_cash_flows['date'] == dates, 'IPD_reset_payment'] = +IPD_reset_payment

        self.daily_spv_cashflows = self.daily_spv_cashflows.merge(
            self.TA_cash_flows[['date', 'IPD_reset_payment']].groupby(['date']).sum().reset_index(),
            how='left',
            on=['date']
        )

    def generate_FA_cash_flows(self):

        print('Generating ' + self.creditor_id + ' funding account balances...')

        origination = self.daily_spv_cashflows[

            (self.daily_spv_cashflows['creditor_id'] == self.creditor_id) &
            (self.daily_spv_cashflows['country_code'] == self.country_code) &
            (self.daily_spv_cashflows['currency_code'] == self.currency_code)

            ][['date', 'country_code', 'currency_code', 'creditor_id', 'origination']].groupby(
            ['date', 'country_code', 'currency_code', 'creditor_id']).sum().reset_index()

        draw_down_agg = self.draw_down_schedule[['Settlement date', 'Currency', 'Amount']].groupby(
            ['Settlement date', 'Currency']).sum().reset_index().rename(

            columns={'Settlement date': 'date',
                     'Currency': 'currency_code',
                     'Amount': 'draw_down_amount'
                     }
        )

        self.FA_cash_flows = self.spv_output_template.merge(
            self.TA_cash_flows[['date', 'country_code', 'currency_code', 'creditor_id', 'IPD_reset_payment']],
            how='left',
            on=['date', 'country_code', 'currency_code', 'creditor_id']
        ).merge(
            draw_down_agg,
            how='left',
            on=['date', 'currency_code']
        ).merge(
            origination,
            how='left',
            on=['date', 'country_code', 'currency_code', 'creditor_id']
        )
        self.FA_cash_flows['IPD_reset_payment'] *= -1

        for dates in self.FA_cash_flows.sort_values(by='date')['date'].drop_duplicates().to_list():

            if dates == self.FA_cash_flows['date'].min():

                account_balance_yesterday = self.FA_start

            else:

                account_balance_yesterday = \
                    self.FA_cash_flows[self.FA_cash_flows['date'] == dates + pd.DateOffset(days=-1)][
                        'account_balance'].fillna(0).values

            self.FA_cash_flows.loc[self.FA_cash_flows['date'] == dates, 'account_balance'] = account_balance_yesterday + \
                                                                                             self.FA_cash_flows[
                                                                                                 'origination'].fillna(
                                                                                                 0) + \
                                                                                             self.FA_cash_flows[
                                                                                                 'IPD_reset_payment'].fillna(
                                                                                                 0) + \
                                                                                             self.FA_cash_flows[
                                                                                                 'draw_down_amount'].fillna(
                                                                                                 0)

    def calculate_funding_balances_at_fc_start(self):

        draw_down_agg = self.draw_down_schedule[['Settlement date', 'Lender', 'Currency', 'Amount']].groupby(
            ['Settlement date', 'Lender', 'Currency']).sum().reset_index().rename(

            columns={'Settlement date': 'date',
                     'Currency': 'currency_code',
                     'Lender': 'lender_id',
                     'Amount': 'balance'
                     }
        )

        balances_at_date = self.lender_amounts_at_date.groupby(
            ['Balance date', 'Lender', 'Balance', 'Currency']).sum().reset_index().rename(

            columns={'Lender': 'lender_id',
                     'Balance': 'balance',
                     'Currency': 'currency_code',
                     'Balance date': 'date'
                     })

        self.lender_balance_at_fc_start = pd.DataFrame()

        for lenders in set(draw_down_agg['lender_id'].drop_duplicates().to_list() + balances_at_date[
            'lender_id'].drop_duplicates().to_list()):
            balance_date_i = balances_at_date[balances_at_date['lender_id'] == lenders]['date'].max()

            included_draw_downs = draw_down_agg[

                (draw_down_agg['lender_id'] == lenders) &
                (draw_down_agg['date'] > balance_date_i) &
                (draw_down_agg['date'] < self.portfolio_forecast.fc_start_date)
                ]

            self.lender_balance_at_fc_start = pd.concat([

                self.lender_balance_at_fc_start,

                balances_at_date,
                included_draw_downs
            ])

            self.lender_balance_at_fc_start = self.lender_balance_at_fc_start.groupby(
                ['lender_id', 'currency_code']).sum().reset_index()

    def generate_funding_balances(self):

        print('Generating ' + self.creditor_id + ' debt balances...')

        draw_down_agg = self.draw_down_schedule[['Settlement date', 'Lender', 'Currency', 'Amount']].groupby(
            ['Settlement date', 'Lender', 'Currency']).sum().reset_index().rename(

            columns={'Settlement date': 'date',
                     'Currency': 'currency_code',
                     'Lender': 'lender_id',
                     'Amount': 'draw_down_amount'
                     }
        )

        lender_list = self.lender_balance_at_fc_start['lender_id'].drop_duplicates().to_list()
        date_list = self.spv_output_template['date'].drop_duplicates().to_list()

        self.liabilities = pd.DataFrame(
            list(itertools.product(date_list, lender_list)),
            columns=['date', 'lender_id']
        ).drop_duplicates().merge(
            draw_down_agg,
            how='left',
            on=['date', 'lender_id']
        ).assign(currency_code=self.currency_code).fillna(0)

        for dates in self.liabilities.sort_values(by='date')['date']:

            for lenders in self.lender_balance_at_fc_start['lender_id']:

                if dates == self.liabilities['date'].min():

                    opening_lender_balance = \
                        self.lender_balance_at_fc_start[self.lender_balance_at_fc_start['lender_id'] == lenders][
                            'balance'].values

                else:

                    opening_lender_balance = self.liabilities[

                        (self.liabilities['date'] == dates - pd.DateOffset(days=1)) &
                        (self.liabilities['lender_id'] == lenders)

                        ]['amount'].values

                lender_balance = opening_lender_balance + self.liabilities[
                    (self.liabilities['date'] == dates) &
                    (self.liabilities['lender_id'] == lenders)
                    ]['draw_down_amount'].fillna(0).values

                self.liabilities.loc[
                    (self.liabilities['date'] == dates) &
                    (self.liabilities['lender_id'] == lenders),
                    'amount'
                ] = lender_balance

    def return_applicable_spv_balance_date(self, date):

        if (self.portfolio_forecast.date_is_holiday(date) == False and date.weekday() < 5):

            return date + pd.DateOffset(days=-1)

        else:

            return self.return_nearest_business_day(
                self.return_nearest_business_day(date, preceeding=True) + pd.DateOffset(days=-1),
                preceeding=True
            )

    def generate_balance_sheet(self):

        print('Generating ' + self.creditor_id + ' balance sheet...')

        total_spv_loan_balance = self.portfolio_forecast.credit_risk_indicator_output[
            (self.portfolio_forecast.credit_risk_indicator_output['creditor_id'] == self.creditor_id) &
            (self.portfolio_forecast.credit_risk_indicator_output['country_code'] == self.country_code) &
            (self.portfolio_forecast.credit_risk_indicator_output['currency_code'] == self.currency_code)
            ][['date', 'country_code', 'currency_code', 'creditor_id', 'principal_balance']].groupby(
            ['date', 'country_code', 'currency_code', 'creditor_id']).sum().reset_index()

        total_spv_loan_balance['nearest_preceeding_business_day'] = total_spv_loan_balance.apply(
            lambda x: self.return_applicable_spv_balance_date(x.date), axis=1)
        total_spv_loan_balance = total_spv_loan_balance.merge(
            total_spv_loan_balance.drop(['nearest_preceeding_business_day'], axis=1),
            how='left',
            left_on=['nearest_preceeding_business_day', 'country_code', 'currency_code', 'creditor_id'],
            right_on=['date', 'country_code', 'currency_code', 'creditor_id'],
            suffixes=('', '_pre')
        )
        total_spv_loan_balance['principal_balance'] = total_spv_loan_balance['principal_balance_pre']
        total_spv_loan_balance = total_spv_loan_balance.drop(
            ['principal_balance_pre', 'nearest_preceeding_business_day', 'date_pre'], axis=1)
        total_spv_loan_balance = total_spv_loan_balance[
            total_spv_loan_balance['date'] >= self.portfolio_forecast.fc_start_date]

        self.eligible_balances = self.portfolio_forecast.credit_risk_indicator_output[
            (self.portfolio_forecast.credit_risk_indicator_output['creditor_id'] == self.creditor_id) &
            (self.portfolio_forecast.credit_risk_indicator_output['country_code'] == self.country_code) &
            (self.portfolio_forecast.credit_risk_indicator_output['currency_code'] == self.currency_code) &
            (self.portfolio_forecast.credit_risk_indicator_output['eligibility_type'] != '90+') &
            (self.portfolio_forecast.credit_risk_indicator_output['eligibility_type'] != 'missed_first_payment')
            ][['date', 'country_code', 'currency_code', 'creditor_id', 'principal_balance']].groupby(
            ['date', 'country_code', 'currency_code', 'creditor_id']).sum().reset_index()
        self.eligible_balances['nearest_preceeding_business_day'] = self.eligible_balances.apply(
            lambda x: self.return_applicable_spv_balance_date(x.date), axis=1)
        self.eligible_balances = self.eligible_balances.merge(
            self.eligible_balances.drop(['nearest_preceeding_business_day'], axis=1),
            how='left',
            left_on=['nearest_preceeding_business_day', 'country_code', 'currency_code', 'creditor_id'],
            right_on=['date', 'country_code', 'currency_code', 'creditor_id'],
            suffixes=('', '_pre')
        )
        self.eligible_balances['principal_balance'] = self.eligible_balances['principal_balance_pre']
        self.eligible_balances = self.eligible_balances.drop(
            ['principal_balance_pre', 'nearest_preceeding_business_day', 'date_pre'], axis=1)
        self.eligible_balances = self.eligible_balances[
            self.eligible_balances['date'] >= self.portfolio_forecast.fc_start_date]

        self.non_eligible_balances = self.spv_output_template.merge(
            total_spv_loan_balance,
            how='left',
            on=['date', 'country_code', 'currency_code', 'creditor_id']
        ).merge(
            self.eligible_balances,
            how='left',
            on=['date', 'country_code', 'currency_code', 'creditor_id'],
            suffixes=('', '_eligible')
        )
        self.non_eligible_balances['amount'] = self.non_eligible_balances['principal_balance'] - \
                                               self.non_eligible_balances['principal_balance_eligible']
        self.non_eligible_balances = self.non_eligible_balances.drop(
            ['principal_balance', 'principal_balance_eligible'], axis=1)
        self.non_eligible_balances = self.non_eligible_balances.assign(
            balance_sheet_lvl_1='Assets',
            balance_sheet_lvl_2='Loans',
            balance_sheet_lvl_3='Non-eligible loans'
        )

        self.eligible_balances = self.eligible_balances.assign(
            balance_sheet_lvl_1='Assets',
            balance_sheet_lvl_2='Loans',
            balance_sheet_lvl_3='Eligible loans'
        ).rename(columns={'principal_balance': 'amount'})

        cash_balances = pd.concat([

            self.FA_cash_flows[['date', 'country_code', 'currency_code', 'creditor_id', 'account_balance']].rename(
                columns={'account_balance': 'amount'}).assign(
                balance_sheet_lvl_1='Assets',
                balance_sheet_lvl_2='Cash',
                balance_sheet_lvl_3='Funding account'
            ),

            self.TA_cash_flows[['date', 'country_code', 'currency_code', 'creditor_id', 'account_balance']].rename(
                columns={'account_balance': 'amount'}).assign(
                balance_sheet_lvl_1='Assets',
                balance_sheet_lvl_2='Cash',
                balance_sheet_lvl_3='Transaction account'
            )

        ])

        spv_liabilities = self.liabilities.assign(
            balance_sheet_lvl_1='Liabilities',
            balance_sheet_lvl_2=self.liabilities['lender_id'],
            balance_sheet_lvl_3=self.liabilities['lender_id'],
            country_code=self.country_code,
            creditor_id=self.creditor_id
        )[['date', 'country_code', 'currency_code', 'creditor_id', 'amount', 'balance_sheet_lvl_1',
           'balance_sheet_lvl_2', 'balance_sheet_lvl_3']]

        cumulative_losses = self.spv_output_template[['date']].drop_duplicates().assign(
            creditor_id=self.creditor_id,
            balance_sheet_lvl_1='Liabilities',
            balance_sheet_lvl_2=self.loss_absorbing_lender,
            balance_sheet_lvl_3=self.loss_absorbing_lender + ' (absorbed losses)',
            amount=-self.cumulative_losses
        )

        self.balance_sheet = pd.concat([

            cash_balances,
            self.non_eligible_balances,
            self.eligible_balances,
            spv_liabilities,
            cumulative_losses

        ])

        tot_assets = self.balance_sheet[self.balance_sheet['balance_sheet_lvl_1'] == 'Assets'][
            ['date', 'amount']].groupby(['date']).sum().reset_index()
        tot_liabilities = self.balance_sheet[self.balance_sheet['balance_sheet_lvl_1'] == 'Liabilities'][
            ['date', 'amount']].groupby(['date']).sum().reset_index()

        balancing_item = tot_assets.merge(
            tot_liabilities,
            how='left',
            on=['date'],
            suffixes=('_ass', '_liab')
        )
        balancing_item['amount'] = balancing_item['amount_ass'] - balancing_item['amount_liab']
        balancing_item.loc[
            balancing_item['amount_ass'] > balancing_item['amount_liab'], 'balance_sheet_lvl_1'] = 'Liabilities'
        balancing_item.loc[
            balancing_item['amount_ass'] <= balancing_item['amount_liab'], 'balance_sheet_lvl_1'] = 'Assets'
        balancing_item.loc[balancing_item['amount_ass'] < balancing_item['amount_liab'], 'amount'] *= -1

        balancing_item = balancing_item.assign(
            country_code=self.country_code,
            currency_code=self.currency_code,
            balance_sheet_lvl_2='unknown',
            balance_sheet_lvl_3='unknown',
            creditor_id=self.creditor_id
        ).drop(['amount_ass', 'amount_liab'], axis=1)

        self.balance_sheet = pd.concat([
            self.balance_sheet,
            balancing_item
        ])

        self.balance_sheet = self.balance_sheet[
            self.balance_sheet['date'] <= self.portfolio_forecast.fc_end_date_output]

        self.daily_spv_cashflows = self.daily_spv_cashflows[
            self.daily_spv_cashflows['date'] <= self.portfolio_forecast.fc_end_date_output]

    def generate_complete_spv_forecast(self):

        self.generate_output_template()
        self.generate_spv_cashflows()
        self.generate_TA_cash_flows()
        self.generate_FA_cash_flows()
        self.calculate_funding_balances_at_fc_start()
        self.generate_funding_balances()
        self.generate_balance_sheet()
