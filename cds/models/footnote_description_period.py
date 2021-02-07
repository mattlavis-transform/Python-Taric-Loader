import sys

import common.globals as g
from common.database import Database
from cds.models.master import Master
from cds.models.footnote_description import FootnoteDescription


class FootnoteDescriptionPeriod(Master):
    def __init__(self, elem, footnote_type_id, footnote_id, import_file):
        Master.__init__(self, elem)
        self.footnote_type_id = footnote_type_id
        self.footnote_id = footnote_id
        self.footnote_description_period_sid = Master.process_null(elem.find("sid"))
        self.validity_start_date = Master.process_null(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_null(elem.find("validityEndDate"))
        operation_date = g.app.get_timestamp()

        # Insert the footnote description period
        sql = """
        insert into footnote_description_periods_oplog
        (footnote_description_period_sid, footnote_type_id, footnote_id, validity_start_date, validity_end_date,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = [
            self.footnote_description_period_sid,
            self.footnote_type_id,
            self.footnote_id,
            self.validity_start_date,
            self.validity_end_date,
            self.operation,
            operation_date,
            operation_date,
            import_file
        ]
        d = Database()
        d.run_query(sql, params)

        # Create new foonote descriptions
        for elem in elem.findall('.//footnoteDescription'):
            FootnoteDescription(elem, self.footnote_description_period_sid, self.footnote_type_id, self.footnote_id, import_file)
