import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master


class QuotaSuspensionPeriod(Master):
    def __init__(self, elem, quota_definition_sid, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.quota_suspension_period_sid = Master.process_null(elem.find("sid"))
        self.quota_definition_sid = quota_definition_sid
        self.suspension_start_date = Master.process_date(elem.find("suspensionStartDate"))
        self.suspension_end_date = Master.process_date(elem.find("suspensionEndDate"))
        self.description = Master.process_null(elem.find("description"))
        operation_date = g.app.get_timestamp()

        if transform_only is False:
            sql = """
            insert into quota_suspension_periods_oplog
            (quota_suspension_period_sid, quota_definition_sid,
            suspension_start_date, suspension_end_date, description,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.quota_suspension_period_sid,
                self.quota_definition_sid,
                self.suspension_start_date,
                self.suspension_end_date,
                self.description,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)
