import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master


class FootnoteAssociationMeasure(Master):
    def __init__(self, elem, measure_sid, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.measure_sid = measure_sid
        self.footnote_type_id = Master.process_null(elem.find("footnote/footnoteType/footnoteTypeId"))
        self.footnote_id = Master.process_null(elem.find("footnote/footnoteId"))

        operation_date = g.app.get_timestamp()
        if transform_only is False:
            sql = """
            insert into footnote_association_measures_oplog
            (measure_sid, footnote_type_id, footnote_id,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.measure_sid,
                self.footnote_type_id,
                self.footnote_id,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)
