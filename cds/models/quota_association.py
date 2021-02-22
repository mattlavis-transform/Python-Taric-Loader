import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master


class QuotaAssociation(Master):
    def __init__(self, elem, quota_definition_sid, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.quota_definition_sid = quota_definition_sid
        self.sub_quota_definition_sid = Master.process_null(elem.find("subQuotaDefinition/sid"))
        self.relation_type = Master.process_null(elem.find("relationType"))
        self.coefficient = Master.process_null(elem.find("coefficient"))
        operation_date = g.app.get_timestamp()

        g.app.print_only("Running operation {0} on quota association {1}.".format(self.operation, str(self.quota_definition_sid)))

        if transform_only is False:
            sql = """
            insert into quota_associations_oplog
            (main_quota_definition_sid, sub_quota_definition_sid, relation_type, coefficient,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.quota_definition_sid,
                self.sub_quota_definition_sid,
                self.relation_type,
                self.coefficient,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)
