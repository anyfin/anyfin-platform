import xlsxwriter
from openpyxl import load_workbook
import datetime as dt
import pandas as pd
import os

class Settings:
    
    def __init__(self, load_new_data, generate_new_settings_template, spv_name):
        
        self.load_new_data = load_new_data
        
        if generate_new_settings_template == True:
            
            self.generate_settings_excel_template()

        date_settings = pd.read_excel(os.path.join(os.path.dirname(__file__), os.path.join(os.path.dirname(__file__), 'Model settings.xlsx')), 'Date settings', header = None, parse_dates = [1])
        self.fc_start_date = pd.Timestamp(date_settings[date_settings[0] == 'Forecast start date'][1].values[0])
        self.fc_end_date = pd.Timestamp(date_settings[date_settings[0] == 'Forecast end date'][1].values[0])

        fx_settings = pd.read_excel(os.path.join(os.path.dirname(__file__), 'Model settings.xlsx'), 'FX rates', header = None)
        self.SEK_USD_plan_rate = fx_settings[fx_settings[0] == 'SEK/USD rate'][1].values[0]
        self.SEK_EUR_plan_rate = fx_settings[fx_settings[0] == 'SEK/EUR plan rate'][1].values[0]
        self.EUR_USD_plan_rate = fx_settings[fx_settings[0] == 'EUR/USD plan rate'][1].values[0]
        
        self.included_countries = pd.read_excel(os.path.join(os.path.dirname(__file__), 'Model settings.xlsx'), 'Included countries')['Country'].to_list()
        
        self.creditor_split_new_customers = pd.read_excel(os.path.join(os.path.dirname(__file__), 'Model settings.xlsx'), 'Creditor split new customers', parse_dates = ['Apply from date'])
            
        spv_settings = pd.read_excel(os.path.join(os.path.dirname(__file__), 'Model settings.xlsx'), 'Settings ' + spv_name, header = None)
        self.IPD_reset_day = spv_settings[spv_settings[0] == 'IPD reset day'][1].values[0]
        self.IPD_reset_payment_day = spv_settings[spv_settings[0] == 'IPD reset payment day'][1].values[0]
        self.creditor_id = spv_name
        self.country_code = spv_settings[spv_settings[0] == 'Country'][1].values[0]
        self.currency_code = spv_settings[spv_settings[0] == 'Currency'][1].values[0]
        
        spv_balances = pd.read_excel(os.path.join(os.path.dirname(__file__), 'Model settings.xlsx'), 'Balances ' + spv_name, header = None)
        self.balance_as_of_latest_IPD_reset_date = spv_balances[spv_balances[0] == 'Balance at latest IPD reset date'][1].values[0]
        self.TA_start = spv_balances[spv_balances[0] == 'Transaction account start balance'][1].values[0]
        self.FA_start = spv_balances[spv_balances[0] == 'Funding account start balance'][1].values[0]
        
        self.lender_amounts_at_date = pd.read_excel(os.path.join(os.path.dirname(__file__), 'Model settings.xlsx'), 'Lenders ' + spv_name, parse_dates = ['Balance date'])
        
        self.draw_down_schedule = pd.read_excel(os.path.join(os.path.dirname(__file__), 'Model settings.xlsx'), 'Draw downs ' + spv_name, parse_dates = ['Drawdown date', 'Settlement date'])
            
    def generate_settings_excel_template(self):
        
        wb = xlsxwriter.Workbook(os.path.join(os.path.dirname(__file__), 'Model settings.xlsx'))
        
        date_tab = wb.add_worksheet('Date settings')
        date_tab.write(0,0,'Forecast start date')
        date_tab.write(0,1, dt.date.today().strftime('%Y-%m-%d'))
        date_tab.write(1,0,'Forecast end date')
        date_tab.write(1,1, (dt.date.today() + pd.DateOffset(months = 6)).strftime('%Y-%m-%d'))
        
        FX_tab = wb.add_worksheet('FX rates')
        FX_tab.write(0,0,'SEK/USD rate')
        FX_tab.write(1,0,'SEK/EUR plan rate')
        FX_tab.write(2,0,'EUR/USD plan rate')
        FX_tab.write(0,1,'0.1015')
        FX_tab.write(1,1,'10.39')
        FX_tab.write(2,1,'1.05')       
        
        country_tab = wb.add_worksheet('Included countries')
        country_tab.write(0,0,'Country')
        country_tab.write(1,0,'SE')
        country_tab.write(2,0,'DE')
        country_tab.write(3,0,'FI')
        
        creditor_tab = wb.add_worksheet('Creditor split new customers')
        creditor_tab.write(0,0, 'Country')
        creditor_tab.write(0,1, 'Creditor')
        creditor_tab.write(0,2, 'Apply from date')
        creditor_tab.write(0,3, 'Share of new customer loans')
        
        creditor_tab.write(1,0, 'SE')
        creditor_tab.write(1,1, 'anyfin-finance-1')
        creditor_tab.write(1,2, '2022-01-01')
        creditor_tab.write(1,3, '1')
        
        creditor_tab.write(2,0, 'DE')
        creditor_tab.write(2,1, 'erikpenser-germany')
        creditor_tab.write(2,2, '2022-01-01')
        creditor_tab.write(2,3, '1')

        creditor_tab.write(3,0, 'FI')
        creditor_tab.write(3,1, 'erikpenser-finland-ff')
        creditor_tab.write(3,2, '2022-01-01')
        creditor_tab.write(3,3, '1')
            
        SPV_settings_tab = wb.add_worksheet('Settings anyfin-finance-1')
        SPV_settings_tab.write(0,0,'IPD reset day')
        SPV_settings_tab.write(0,1,'7')
        SPV_settings_tab.write(1,0,'IPD reset payment day')
        SPV_settings_tab.write(1,1,'18')
        SPV_settings_tab.write(2,0,'Country')
        SPV_settings_tab.write(2,1,'SE')
        SPV_settings_tab.write(3,0,'Currency')
        SPV_settings_tab.write(3,1,'SEK')
        
        SPV_balances_tab = wb.add_worksheet('Balances anyfin-finance-1')
        SPV_balances_tab.write(0,0,'Balance at latest IPD reset date')
        SPV_balances_tab.write(0,1,'38000000')
        SPV_balances_tab.write(1,0,'Transaction account start balance')
        SPV_balances_tab.write(1,1,'30000000')
        SPV_balances_tab.write(2,0,'Funding account start balance')
        SPV_balances_tab.write(2,1,'0')
        
        SPV_draw_downs_tab = wb.add_worksheet('Draw downs anyfin-finance-1')
        SPV_draw_downs_tab.write(0,0, 'Drawdown date')
        SPV_draw_downs_tab.write(0,1, 'Settlement date')
        SPV_draw_downs_tab.write(0,2, 'Lender')
        SPV_draw_downs_tab.write(0,3, 'Type of loan')
        SPV_draw_downs_tab.write(0,4, 'Amount')
        SPV_draw_downs_tab.write(0,5, 'Currency')
        SPV_draw_downs_tab.write(0,6, 'Account allocation')
        SPV_draw_downs_tab.write(0,7, 'Interest rate')
        
        SPV_draw_downs_tab.write(1,0, '2022-05-31')
        SPV_draw_downs_tab.write(1,1, '2022-06-03')
        SPV_draw_downs_tab.write(1,2, 'Anyfin AB')
        SPV_draw_downs_tab.write(1,3, 'Subordinated')
        SPV_draw_downs_tab.write(1,4, '10000000')
        SPV_draw_downs_tab.write(1,5, 'SEK')
        SPV_draw_downs_tab.write(1,6, 'Funding account')
        SPV_draw_downs_tab.write(1,7, '0.2')
                    
        SPV_draw_downs_tab.write(2,0, '2021-05-12')
        SPV_draw_downs_tab.write(2,1, '2021-05-14')
        SPV_draw_downs_tab.write(2,2, 'Anyfin AB')
        SPV_draw_downs_tab.write(2,3, 'Junior')
        SPV_draw_downs_tab.write(2,4, '10000000')
        SPV_draw_downs_tab.write(2,5, 'SEK')
        SPV_draw_downs_tab.write(2,6, 'Funding account')
        SPV_draw_downs_tab.write(2,7, '0.1')

        SPV_draw_downs_tab.write(2,0, '2021-06-01')
        SPV_draw_downs_tab.write(2,1, '2021-06-01')
        SPV_draw_downs_tab.write(2,2, 'Barclays Bank Ireland Plc')
        SPV_draw_downs_tab.write(2,3, 'Senior')
        SPV_draw_downs_tab.write(2,4, '20000000')
        SPV_draw_downs_tab.write(2,5, 'SEK')
        SPV_draw_downs_tab.write(2,6, 'Funding account')
        SPV_draw_downs_tab.write(2,7, '0.0225')
        
        SPV_lenders = wb.add_worksheet('Lenders anyfin-finance-1')
        SPV_lenders.write(0,0, 'Lender')
        SPV_lenders.write(0,1, 'Currency')
        SPV_lenders.write(0,2, 'Balance')
        SPV_lenders.write(0,3, 'Balance date')
        SPV_lenders.write(1,0, 'Scandinavian Credit Fund 1 AB')
        SPV_lenders.write(2,0, 'Anyfin AB')
        SPV_lenders.write(3,0, 'Barclays Bank Ireland Plc')
        SPV_lenders.write(1,1, 'SEK')
        SPV_lenders.write(2,1, 'SEK')
        SPV_lenders.write(3,1, 'SEK')
        SPV_lenders.write(1,2, '0')
        SPV_lenders.write(2,2, '0')
        SPV_lenders.write(3,2, '0')
        SPV_lenders.write(1,3, '1900-01-01')
        SPV_lenders.write(2,3, '1900-01-01')
        SPV_lenders.write(3,3, '1900-01-01')
        
        wb.close()