import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.quota_order_number_origin_exclusion import QuotaOrderNumberOriginExclusion


class QuotaOrderNumberOrigin(Master):
    def __init__(self, elem, quota_order_number_sid, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.quota_order_number_sid = quota_order_number_sid
        self.quota_order_number_origin_sid = Master.process_null(elem.find("sid"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        self.geographical_area_id = Master.process_null(elem.find("geographicalArea/geographicalAreaId"))

        operation_date = g.app.get_timestamp()

        # Insert the quota order number origin
        if transform_only is False:
            sql = """
            insert into quota_order_number_origins_oplog
            (quota_order_number_sid, quota_order_number_origin_sid, geographical_area_id,
            validity_start_date, validity_end_date,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.quota_order_number_sid,
                self.quota_order_number_origin_sid,
                self.geographical_area_id,
                self.validity_start_date,
                self.validity_end_date,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)

            # Create new footnote descriptions
            for elem in elem.findall('.//quotaOrderNumberOriginExclusions'):
                QuotaOrderNumberOriginExclusion(elem, self.quota_order_number_origin_sid, import_file)
