import xlsxwriter


class PersonalDataReport:
    def __init__(self, id, results, *args):
        self.personal_id = id
        self.results = results
        self.__initiate_report()

    def __initiate_report(self):
        self.filename = f"{self.personal_id}_gdpr_report.xlsx"
        self.wb = xlsxwriter.Workbook(
            self.filename,
            {
                "constant_memory": True,
                "default_date_format": "yyyy-mm-dd",
            },
        )
        self.__prepare_sheets()

    def __prepare_sheets(self):
        self.sheets = [
            {
                "name": "main/contact_details",
                "sheet": self.wb.add_worksheet("Contact details"),
                "columns": "created_at type value is_active".split(" "),
            },
            {
                "name": "main/application_details",
                "sheet": self.wb.add_worksheet("Application details"),
                "columns": "created_at source customer_name customer_street customer_postal \
                    lender loan_balance currency_code payment_account payment_reference \
                    status reject_reason external_status application_status".split(
                    " "
                ),
            },
            {
                "name": "main/general_customer_details",
                "sheet": self.wb.add_worksheet("General customer information"),
                "columns": "full_name personal_identifier address_street address_postcode \
                    address_city birthdate gender".split(
                    " "
                ),
            },
            {
                "name": "main/autogiro",
                "sheet": self.wb.add_worksheet("Autogiro"),
                "columns": "created_at personal_identifier account_number status".split(
                    " "
                ),
            },
            {
                "name": "main/signatures",
                "sheet": self.wb.add_worksheet("Signatures"),
                "columns": "created_at signee_name signee_pno signee_ip signed_text platform \
                    user_ip user_agent".split(
                    " "
                ),
            },
            {
                "name": "main/external_lookup_listings",
                "sheet": self.wb.add_worksheet("External lookup listings"),
                "columns": "created_at spar_id spar_ts uc_id uc_ts".split(" "),
            },
            {
                "name": "main/internal_notes",
                "sheet": self.wb.add_worksheet("Internal notes"),
                "columns": "id created_at created_by content customer_id application_id files \
                    type".split(
                    " "
                ),
            },
        ]

    def generate(self):
        for sheet in self.sheets:
            ws = sheet["sheet"]
            name = sheet["name"]
            headers = sheet["columns"]
            for col, header in enumerate(headers):
                ws.write(0, col, header)

            for i, row in enumerate(self.results[name], start=1):
                for j, value in enumerate(row, start=0):
                    print(value)
                    ws.write(i, j, value)
        self.wb.close()
        return self.filename
