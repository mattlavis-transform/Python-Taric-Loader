import sys

import common.globals as g
from common.database import Database
from cds.models.master import Master


class MeasureExcludedGeographicalArea(Master):
    def __init__(self, elem, measure_sid, import_file):
        Master.__init__(self, elem)
        self.measure_sid = measure_sid
        self.excluded_geographical_area = Master.process_null(elem.find("geographicalArea/geographicalAreaId"))
        self.geographical_area_sid = Master.process_null(elem.find("geographicalArea/sid"))

        operation_date = g.app.get_timestamp()

        # Insert the MeasureExcludedGeographicalArea
        sql = """
        insert into measure_excluded_geographical_areas_oplog
        (measure_sid, excluded_geographical_area, geographical_area_sid,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        params = [
            self.measure_sid,
            self.excluded_geographical_area,
            self.geographical_area_sid,
            self.operation,
            operation_date,
            operation_date,
            import_file
        ]
        d = Database()
        d.run_query(sql, params)
