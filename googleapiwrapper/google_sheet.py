import logging
from pprint import pformat
from typing import List

import gspread
from gspread import SpreadsheetNotFound, WorksheetNotFound
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials

COLS = 10
ROWS = 1000

LOG = logging.getLogger(__name__)
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]


class CellUpdateForIssue:
    def __init__(self, issue, update_date_cell, status_cell):
        self.issue = issue
        self.update_date_cell = update_date_cell
        self.status_cell = status_cell

    def __repr__(self):
        return repr((self.issue, self.update_date_cell, self.status_cell))

    def __str__(self):
        return (
            self.__class__.__name__
            + " { issue: "
            + self.issue
            + ", update_date_cell: "
            + str(self.update_date_cell)
            + ", status_cell: "
            + str(self.status_cell)
            + " }"
        )


class GSheetOptions:
    def __init__(
        self,
        client_secret,
        spreadsheet: str,
        worksheet: str = None,
        jira_column=None,
        update_date_column=None,
        status_column=None,
    ):
        self.client_secret = client_secret
        self.spreadsheet: str = spreadsheet
        self.worksheets: List[str] = []
        if worksheet:
            self.worksheets.append(worksheet)
        self.jira_column = jira_column
        self.update_date_column = update_date_column
        self.status_column = status_column

        if self.update_date_column:
            self.do_update_date = True
        if self.status_column:
            self.do_update_status = True

    def add_worksheet(self, ws: str):
        self.worksheets.append(ws)

    @property
    def single_worksheet_mode(self):
        return len(self.worksheets) == 1

    def __repr__(self):
        return repr(
            (
                self.client_secret,
                self.spreadsheet,
                self.worksheets,
                self.jira_column,
                self.update_date_column,
                self.status_column,
            )
        )

    def __str__(self):
        return (
            self.__class__.__name__ + " { "
            "spreadsheet: "
            + self.spreadsheet
            + ", worksheets: "
            + str(self.worksheets)
            + ", jira_column: "
            + self.jira_column
            + ", update_date_column: "
            + self.update_date_column
            + ", status_column: "
            + str(self.status_column)
            + " }"
        )


class GSheetWrapper:
    A1 = "A1"
    DEFAULT_RANGE_TO_CLEAR = "A1:Z10000"

    def __init__(self, options: GSheetOptions):
        LOG.debug(f"GSheetWrapper options: {options}")
        if not isinstance(options, GSheetOptions):
            raise ValueError("options must be an instance of GSheetOptions!")
        if not options.worksheets:
            raise ValueError(
                f"Parameter options (type of {GSheetOptions.__name__} must include 1 or more worksheets "
                f"but it does not have any assigned."
            )

        LOG.debug("GSheetWrapper options: %s", str(options))
        self.options: GSheetOptions = options
        self.multi_worksheet = True if len(self.options.worksheets) > 1 else False

        if not options.client_secret:
            raise ValueError("Client secret should be specified!")

        self.creds = ServiceAccountCredentials.from_json_keyfile_name(options.client_secret, SCOPE)
        self.client = gspread.authorize(self.creds)
        self.issue_to_cellupdate = {}

    def read_data(self, worksheet_name: str, range=None) -> List[List[str]]:
        try:
            sheet = self.client.open(self.options.spreadsheet)
            worksheet = sheet.worksheet(worksheet_name)
            return worksheet.get(range)
        except SpreadsheetNotFound:
            raise ValueError("Spreadsheet was not found with name '{}'".format(self.options.spreadsheet))
        except WorksheetNotFound:
            raise ValueError("Worksheet was not found with name '{}'".format(worksheet_name))

    def write_data(
        self, header, data, worksheet_name: str = None, clear_range=True, create_not_existing_worksheet=False
    ):
        worksheet_to_write: str = self.options.worksheets[0]
        if self.multi_worksheet and not worksheet_name:
            raise ValueError(
                "GsheetWrapper is in multi-worksheet mode but worksheet name was not provided "
                f"so can't decide where to write data. Available worksheets: {self.options.worksheets}"
            )

        if worksheet_name:
            worksheet_to_write = worksheet_name
        if worksheet_to_write not in self.options.worksheets:
            raise ValueError(
                f"Provided worksheet name '{worksheet_name}' is not available. "
                f"Available worksheets: {self.options.worksheets}"
            )

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
                    raise ValueError(
                        f"Failed to create worksheet with name '{worksheet_to_write}', "
                        f"rows: {ROWS}, columns: {COLS},"
                    )
            else:
                raise ValueError(msg)

        all_values = [header]
        all_values.extend(data)

        sheet_title = sheet.title
        worksheet_title = worksheet.title
        if clear_range:
            self.clear_range(sheet, sheet_title, worksheet_title)

        col_letter = chr(ord("a") + len(header) - 1).upper()
        rows = len(all_values)
        range_to_update = "{}:{}{}".format(self.A1, col_letter, rows)
        LOG.info(
            "Adding values to sheet '%s', worksheet: '%s', range: '%s'", sheet_title, worksheet_title, range_to_update
        )
        sheet.values_update(
            "{}!{}".format(worksheet_to_write, range_to_update),
            params={"valueInputOption": "RAW"},
            body={"values": all_values},
        )

    def clear_range(self, sheet, sheet_title, worksheet_title):
        range_to_clear = self._get_range_to_clear(self.DEFAULT_RANGE_TO_CLEAR, worksheet_title)
        LOG.info(
            "Clearing all values from sheet '%s', worksheet: '%s', range: '%s'",
            sheet_title,
            worksheet_title,
            range_to_clear,
        )
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

    def fetch_jira_data(self):
        if not self.options.single_worksheet_mode:
            raise ValueError(
                "Fetching Jira data only works with single worksheet mode. Current worksheets: %s",
                self.options.worksheets,
            )
        worksheet = self.options.worksheets[0]
        LOG.info("Fetching jira data from worksheet: %s", worksheet)

        try:
            sheet = self.client.open(self.options.spreadsheet).worksheet(worksheet)
        except SpreadsheetNotFound:
            raise ValueError("Spreadsheet was not found with name '{}'".format(self.options.spreadsheet))
        except WorksheetNotFound:
            raise ValueError("Worksheet was not found with name '{}'".format(worksheet))

        header = sheet.row_values(1)
        LOG.debug("Fetched spreadsheet header: %s", header)

        update_date_col_idx = self._find_column_idx_in_header(header, self.options.update_date_column, "update date")
        if update_date_col_idx < 0:
            self.options.do_update_date = False

        status_col_idx = self._find_column_idx_in_header(header, self.options.status_column, "status")
        if status_col_idx < 0:
            self.options.do_update_status = False

        rows = sheet.get_all_records()
        LOG.debug("Received data from sheet %s: %s", worksheet, pformat(rows))

        # Check column is found
        jira_col = self.options.jira_column
        if rows and len(rows) > 0:
            if jira_col not in rows[0]:
                row0 = rows[0]
                raise ValueError(
                    "Jira column with name '{}' was not found in "
                    "received data! First row of data: {}".format(jira_col, row0)
                )

        issues = []

        # 1 because of 0-based indexing (rows are 1-based)
        # 2 because of header row is the 1st row
        idx_correction_row = 2

        # 1 because of 0-based col indexing from header
        idx_correction_col = 1
        for idx, row in enumerate(rows):
            issue = row[jira_col]
            issues.append(issue)

            update_date_cell_id, status_cell_id = None, None
            if self.options.do_update_date:
                update_date_cell_id = rowcol_to_a1(idx + idx_correction_row, update_date_col_idx + idx_correction_col)
            if self.options.do_update_status:
                status_cell_id = rowcol_to_a1(idx + idx_correction_row, status_col_idx + idx_correction_col)

            # If update is required for any cell, we need to store a CellUpdateForIssue object, otherwise don't store it
            if update_date_cell_id or status_cell_id:
                self.issue_to_cellupdate[issue] = CellUpdateForIssue(issue, update_date_cell_id, status_cell_id)

        LOG.debug("Issue to CellUpdate mappings: %s", self.issue_to_cellupdate)
        LOG.debug("Found Jira issue from GSheet: %s", issues)

        self.sheet = sheet
        return issues

    def _is_column_index_valid(self, header, column_name: str, column_type: str):
        return self._find_column_idx_in_header(header, column_name, column_type) >= 0

    def _find_column_idx_in_header(self, header, column, type_of_column):
        column_idx = -1
        try:
            LOG.debug("Using %s column with name '%s'", type_of_column, self.options.update_date_column)
            column_idx = header.index(column)
        except ValueError:
            LOG.error(
                "Omitting future updates of %s column as " "it was not found in header %s with name '%s'",
                type_of_column,
                header,
                column,
            )
        if column_idx > -1:
            LOG.debug("%s column was found with index: %d", type_of_column, column_idx)
        return column_idx

    def update_issue_with_results(self, issue, date_str, status: str):
        if not self.sheet:
            raise ValueError("Sheet data is not yet fetched! Please invoke 'fetch' method first!")

        if issue not in self.issue_to_cellupdate:
            # TODO This should be an error + list all jira IDs without stored cell IDs
            LOG.info("No cell update will be performed for issue %s", issue)
            return

        cu = self.issue_to_cellupdate[issue]
        if self.options.do_update_date:
            LOG.info(
                "[%s] Updating GSheet cell '%s' with value: '%s' (update date)", issue, cu.update_date_cell, date_str
            )
            self.sheet.update_acell(cu.update_date_cell, date_str)

        if self.options.do_update_status:
            LOG.info("[%s] Updating GSheet cell '%s' with value: '%s' (overall status)", issue, cu.status_cell, status)
            self.sheet.update_acell(cu.status_cell, status)
