import sys

import common.globals as g
from common.database import Database
from cds.models.master import Master
from cds.models.footnote_type_description import FootnoteTypeDescription


class FootnoteType(Master):
    def __init__(self, elem, import_file):
        Master.__init__(self, elem)
        self.footnote_type_id = Master.process_null(elem.find("footnoteTypeId"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        self.application_code = Master.process_null(elem.find("applicationCode"))
        operation_date = g.app.get_timestamp()

        # Insert the footnote type
        sql = """
        insert into footnote_types_oplog
        (footnote_type_id, application_code, validity_start_date, validity_end_date,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = [
            self.footnote_type_id,
            self.application_code,
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
        delete from footnote_type_descriptions_oplog
        where footnote_type_id = %s
        """
        params = [self.footnote_type_id]
        d = Database()
        d.run_query(sql, params)

        # Create new footnote type descriptions
        for elem in elem.findall('.//footnoteTypeDescription'):
            FootnoteTypeDescription(elem, self.footnote_type_id, import_file)
