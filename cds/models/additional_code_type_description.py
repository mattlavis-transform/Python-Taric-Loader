import common.globals as g
from common.database import Database
from cds.models.master import Master


class AdditionalCodeTypeDescription(Master):
    def __init__(self, elem, additional_code_type_id, import_file):
        Master.__init__(self, elem)
        self.additional_code_type_id = additional_code_type_id
        self.description = Master.process_null(elem.find("description"))
        self.language_id = Master.process_null(elem.find("language/languageId"))
        operation_date = g.app.get_timestamp()
        sql = """
        insert into additional_code_type_descriptions_oplog
        (additional_code_type_id, language_id, description,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        params = [
            self.additional_code_type_id,
            self.language_id,
            self.description,
            self.operation,
            operation_date,
            operation_date,
            import_file
        ]
        d = Database()
        d.run_query(sql, params)
