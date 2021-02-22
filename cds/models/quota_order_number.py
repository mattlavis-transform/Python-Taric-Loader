import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.quota_order_number_origin import QuotaOrderNumberOrigin


class QuotaOrderNumber(Master):
    def __init__(self, elem, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.quota_order_number_sid = Master.process_null(elem.find("sid"))
        self.quota_order_number_id = Master.process_null(elem.find("quotaOrderNumberId"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        operation_date = g.app.get_timestamp()

        # Insert the quota order number
        if transform_only is False:
            sql = """
            insert into quota_order_numbers_oplog
            (quota_order_number_sid, quota_order_number_id,
            validity_start_date, validity_end_date,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.quota_order_number_sid,
                self.quota_order_number_id,
                self.validity_start_date,
                self.validity_end_date,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)

            # Delete any quota order number origins
            sql = """
            delete from quota_order_number_origins_oplog
            where quota_order_number_sid = %s
            """
            params = [self.quota_order_number_sid]
            d = Database()
            d.run_query(sql, params)

            # Create new quota order number origins
            for elem in elem.findall('.//quotaOrderNumberOrigin'):
                QuotaOrderNumberOrigin(elem, self.quota_order_number_sid, import_file)
