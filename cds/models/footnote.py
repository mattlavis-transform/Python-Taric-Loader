import sys

import common.globals as g
from common.database import Database
from cds.models.master import Master
from cds.models.footnote_description_period import FootnoteDescriptionPeriod


class Footnote(Master):
    def __init__(self, elem, import_file):
        Master.__init__(self, elem)
        self.descriptions = []
        self.footnote_type_id = Master.process_null(elem.find("footnoteType/footnoteTypeId"))
        self.footnote_id = Master.process_null(elem.find("footnoteId"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        operation_date = g.app.get_timestamp()

        # Insert the footnote
        sql = """
        insert into footnotes_oplog
        (footnote_type_id, footnote_id, validity_start_date, validity_end_date,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = [
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

        # Delete any footnote descriptions
        sql = """
        delete from footnote_descriptions_oplog
        where footnote_type_id = %s and footnote_id = %s
        """
        params = [self.footnote_type_id, self.footnote_id]
        d = Database()
        d.run_query(sql, params)

        # Delete any footnote description periods
        sql = """
        delete from footnote_description_periods_oplog
        where footnote_type_id = %s and footnote_id = %s
        """
        params = [self.footnote_type_id, self.footnote_id]
        d = Database()
        d.run_query(sql, params)

        # Create new footnote description periods
        for elem in elem.findall('.//footnoteDescriptionPeriod'):
            FootnoteDescriptionPeriod(elem, self.footnote_type_id, self.footnote_id, import_file)
