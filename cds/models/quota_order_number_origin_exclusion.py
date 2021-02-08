import common.globals as g
from common.database import Database
from cds.models.master import Master


class QuotaOrderNumberOriginExclusion(Master):
    def __init__(self, elem, quota_order_number_origin_sid, import_file):
        Master.__init__(self, elem)
        self.quota_order_number_origin_sid = quota_order_number_origin_sid
        self.excluded_geographical_area_sid = Master.process_null(elem.find("geographicalArea/sid"))
        operation_date = g.app.get_timestamp()
        sql = """
        insert into quota_order_number_origin_exclusions_oplog
        (quota_order_number_origin_sid, excluded_geographical_area_sid,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = [
            self.quota_order_number_origin_sid,
            self.excluded_geographical_area_sid,
            self.operation,
            operation_date,
            operation_date,
            import_file
        ]
        d = Database()
        d.run_query(sql, params)
