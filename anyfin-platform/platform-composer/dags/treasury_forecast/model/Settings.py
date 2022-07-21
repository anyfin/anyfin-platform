import pandas as pd

class Settings:

    def __init__(self):
        prefix = '/home/airflow/gcs/data/treasury_forecast/'
        general_settings = pd.read_excel(f'{prefix}Model settings.xlsx', 'General settings', header=None)
        self.fc_start_date = pd.Timestamp(general_settings[general_settings[0] == 'Forecast start date'][1].values[0])
        self.fc_end_date = pd.Timestamp(general_settings[general_settings[0] == 'Forecast end date'][1].values[0])
        self.SEK_USD_plan_rate = general_settings[general_settings[0] == 'SEK/USD rate'][1].values[0]
        self.SEK_EUR_plan_rate = general_settings[general_settings[0] == 'SEK/EUR plan rate'][1].values[0]
        self.EUR_USD_plan_rate = general_settings[general_settings[0] == 'EUR/USD plan rate'][1].values[0]
        self.load_new_data = general_settings[general_settings[0] == 'Load fresh data'][1].values[0]
        self.included_countries = pd.read_excel(f'{prefix}Model settings.xlsx', 'Included countries')['Country'].to_list()
        self.new_markets = pd.read_excel(f'{prefix}Model settings.xlsx', 'New markets')
        self.adjustments = pd.read_excel(f'{prefix}Model settings.xlsx', 'Adjustments')
        self.ignored_periods = pd.read_excel(f'{prefix}Model settings.xlsx', 'Ignored periods',
                                             parse_dates=['Ignore period start', 'Ignore period end'])
        self.creditor_split_new_customers = pd.read_excel(f'{prefix}Model settings.xlsx', 'Creditor split new customers',
                                                          parse_dates=['Apply from date'])
        self.spv_settings = pd.read_excel(f'{prefix}Model settings.xlsx', 'SPV settings')
        self.spv_lenders = pd.read_excel(f'{prefix}Model settings.xlsx', 'SPV lenders')
        self.spv_draw_downs = pd.read_excel(f'{prefix}Model settings.xlsx', 'SPV draw downs')
        self.funding_costs_assumptions = pd.read_excel(f'{prefix}Model settings.xlsx', 'Funding costs')
        self.revenue_assumptions = pd.read_excel(f'{prefix}Model settings.xlsx', 'Revenue')
