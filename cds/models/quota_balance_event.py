import common.globals as g
from common.database import Database
from cds.models.master import Master


class QuotaBalanceEvent(Master):
    def __init__(self, elem, quota_definition_sid, import_file):
        Master.__init__(self, elem)
        self.quota_definition_sid = quota_definition_sid
        self.imported_amount = Master.process_null(elem.find("importedAmount"))
        self.new_balance = Master.process_null(elem.find("newBalance"))
        self.occurrence_timestamp = Master.process_date(elem.find("occurrenceTimestamp"))
        self.old_balance = Master.process_null(elem.find("oldBalance"))
        self.last_import_date_in_allocation = Master.process_date(elem.find("lastImportDateInAllocation"))

        operation_date = g.app.get_timestamp()
        sql = """
        insert into quota_balance_events_oplog
        (quota_definition_sid, imported_amount,
        new_balance, occurrence_timestamp, old_balance, last_import_date_in_allocation,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = [
            self.quota_definition_sid,
            self.imported_amount,
            self.new_balance,
            self.occurrence_timestamp,
            self.old_balance,
            self.last_import_date_in_allocation,
            self.operation,
            operation_date,
            operation_date,
            import_file
        ]
        d = Database()
        d.run_query(sql, params)
