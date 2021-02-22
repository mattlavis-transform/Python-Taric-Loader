import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.geographical_area_description import GeographicalAreaDescription


class GeographicalAreaDescriptionPeriod(Master):
    def __init__(self, elem, geographical_area_sid, geographical_area_id, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.geographical_area_sid = geographical_area_sid
        self.geographical_area_id = geographical_area_id
        self.geographical_area_description_period_sid = Master.process_null(elem.find("sid"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        operation_date = g.app.get_timestamp()

        # Insert the geographical area description period
        if transform_only is False:
            sql = """
            insert into geographical_area_description_periods_oplog
            (geographical_area_description_period_sid, geographical_area_sid, geographical_area_id,
            validity_start_date, validity_end_date,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.geographical_area_description_period_sid,
                self.geographical_area_sid,
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

            # Create new geographical area descriptions
            for elem in elem.findall('.//geographicalAreaDescription'):
                GeographicalAreaDescription(elem, self.geographical_area_description_period_sid, self.geographical_area_sid, self.geographical_area_id, import_file)
