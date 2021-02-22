import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master


class GeographicalAreaDescription(Master):
    def __init__(self, elem, geographical_area_description_period_sid, geographical_area_sid, geographical_area_id, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.geographical_area_description_period_sid = geographical_area_description_period_sid
        self.geographical_area_sid = geographical_area_sid
        self.geographical_area_id = geographical_area_id
        self.description = Master.process_null(elem.find("description"))
        self.language_id = Master.process_null(elem.find("language/languageId"))
        operation_date = g.app.get_timestamp()

        if transform_only is False:
            sql = """
            insert into geographical_area_descriptions_oplog
            (geographical_area_description_period_sid, geographical_area_sid, geographical_area_id, language_id, description,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.geographical_area_description_period_sid,
                self.geographical_area_sid,
                self.geographical_area_id,
                self.language_id,
                self.description,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)
