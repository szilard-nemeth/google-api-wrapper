import logging
from typing import List

import gspread
from gspread import SpreadsheetNotFound, WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials

COLS = 10
ROWS = 1000

LOG = logging.getLogger(__name__)
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']


class GSheetOptions:
    def __init__(self, client_secret, spreadsheet: str, worksheet: str = None):
        self.client_secret = client_secret
        self.spreadsheet: str = spreadsheet
        self.worksheets: List[str] = []
        if worksheet:
            self.worksheets.append(worksheet)

    def add_worksheet(self, ws: str):
        self.worksheets.append(ws)

    def __repr__(self):
        return repr((self.client_secret, self.spreadsheet, self.worksheets))

    def __str__(self):
        return self.__class__.__name__ + \
               " { " \
               "spreadsheet: " + self.spreadsheet + \
               ", worksheets: " + str(self.worksheets) + " }"


class GSheetWrapper:
    A1 = "A1"
    DEFAULT_RANGE_TO_CLEAR = 'A1:Z10000'

    def __init__(self, options: GSheetOptions):
        LOG.debug(f"GSheetWrapper options: {options}")
        if not isinstance(options, GSheetOptions):
            raise ValueError('options must be an instance of GSheetOptions!')
        if not options.worksheets:
            raise ValueError(f"Parameter options (type of {GSheetOptions.__name__} must include 1 or more worksheets "
                             f"but it does not have any assigned.")

        self.options = options
        self.multi_worksheet = True if len(self.options.worksheets) > 1 else False

        if not options.client_secret:
            raise ValueError("Client secret should be specified!")

        self.creds = ServiceAccountCredentials.from_json_keyfile_name(options.client_secret, SCOPE)
        self.client = gspread.authorize(self.creds)

    def read_data(self, worksheet_name: str, range=None) -> List[List[str]]:
        try:
            sheet = self.client.open(self.options.spreadsheet)
            worksheet = sheet.worksheet(worksheet_name)
            return worksheet.get(range)
        except SpreadsheetNotFound:
            raise ValueError("Spreadsheet was not found with name '{}'".format(self.options.spreadsheet))
        except WorksheetNotFound:
            raise ValueError("Worksheet was not found with name '{}'".format(worksheet_name))

    def write_data(self, header, data,
                   worksheet_name: str = None,
                   clear_range=True,
                   create_not_existing_worksheet=False):
        worksheet_to_write: str = self.options.worksheets[0]
        if self.multi_worksheet and not worksheet_name:
            raise ValueError("GsheetWrapper is in multi-worksheet mode but worksheet name was not provided "
                             f"so can't decide where to write data. Available worksheets: {self.options.worksheets}")

        if worksheet_name:
            worksheet_to_write = worksheet_name
        if worksheet_to_write not in self.options.worksheets:
            raise ValueError(f"Provided worksheet name '{worksheet_name}' is not available. "
                             f"Available worksheets: {self.options.worksheets}")

        # TODO add new column: Last updated date
        sheet = None
        try:
            sheet = self.client.open(self.options.spreadsheet)
            worksheet = sheet.worksheet(worksheet_to_write)
        except SpreadsheetNotFound:
            raise ValueError("Spreadsheet was not found with name '{}'".format(self.options.spreadsheet))
        except WorksheetNotFound:
            msg = "Worksheet was not found with name '{}'".format(worksheet_to_write)
            if create_not_existing_worksheet:
                LOG.warning(f"{msg}, but create_not_existing_worksheet is enabled so trying to create worksheet now.")
                worksheet = sheet.add_worksheet(worksheet_to_write, ROWS, COLS)
                if worksheet:
                    LOG.info(f"Created worksheet with name '{worksheet_to_write}', rows: {ROWS}, columns: {COLS},")
                else:
                    raise ValueError(f"Failed to create worksheet with name '{worksheet_to_write}', "
                                     f"rows: {ROWS}, columns: {COLS},")
            else:
                raise ValueError(msg)

        all_values = [header]
        all_values.extend(data)

        sheet_title = sheet.title
        worksheet_title = worksheet.title
        if clear_range:
            self.clear_range(sheet, sheet_title, worksheet_title)

        col_letter = chr(ord('a') + len(header) - 1).upper()
        rows = len(all_values)
        range_to_update = "{}:{}{}".format(self.A1, col_letter, rows)
        LOG.info("Adding values to sheet '%s', worksheet: '%s', range: '%s'", sheet_title, worksheet_title, range_to_update)
        sheet.values_update(
            '{}!{}'.format(worksheet_to_write, range_to_update),
            params={'valueInputOption': 'RAW'},
            body={'values': all_values}
        )

    def clear_range(self, sheet, sheet_title, worksheet_title):
        range_to_clear = self._get_range_to_clear(self.DEFAULT_RANGE_TO_CLEAR, worksheet_title)
        LOG.info("Clearing all values from sheet '%s', worksheet: '%s', range: '%s'", sheet_title, worksheet_title,
                 range_to_clear)
        sheet.values_clear(range_to_clear)

        # It seems somehow gspread "memorizes" the cleared range and will add the new rows after the range, regardless
        # of what range have been provided with the range parameter to 'values_update'
        # HACK: Clear range A1:A1
        range_to_clear = self._get_range_to_clear("A1:A1", worksheet_title)
        sheet.values_clear(range_to_clear)

    @staticmethod
    def _get_range_to_clear(range: str, worksheet: str):
        # IMPORTANT: https://developers.google.com/sheets/api/guides/concepts#a1_notation
        # A1:B2 refers to the first two cells in the top two rows of the first visible sheet.
        # Sheet1!A1:B2 refers to the first two cells in the top two rows of Sheet1.
        # The worksheet name has to be included because A1:B2 means the first visible worksheet.
        range_to_clear = f"'{worksheet}'!{range}"
        return range_to_clear
