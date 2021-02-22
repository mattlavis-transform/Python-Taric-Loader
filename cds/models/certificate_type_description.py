import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master


class CertificateTypeDescription(Master):
    def __init__(self, elem, certificate_type_code, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.certificate_type_code = certificate_type_code
        self.description = Master.process_null(elem.find("description"))
        self.language_id = Master.process_null(elem.find("language/languageId"))
        operation_date = g.app.get_timestamp()

        g.app.print_only("Running operation {0} on certificate type description {1}.".format(self.operation, str(self.certificate_type_code)))

        if transform_only is False:
            sql = """
            insert into certificate_type_descriptions_oplog
            (certificate_type_code, language_id, description,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.certificate_type_code,
                self.language_id,
                self.description,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)
