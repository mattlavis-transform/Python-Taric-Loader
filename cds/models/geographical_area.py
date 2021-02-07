import sys

import common.globals as g
from common.database import Database
from cds.models.master import Master
from cds.models.geographical_area_description_period import GeographicalAreaDescriptionPeriod
from cds.models.geographical_area_membership import GeographicalAreaMembership


class GeographicalArea(Master):
    def __init__(self, elem, import_file):
        Master.__init__(self, elem)
        self.hjid = Master.process_null(elem.find("hjid"))
        self.geographical_area_sid = Master.process_null(elem.find("sid"))
        self.geographical_area_id = Master.process_null(elem.find("geographicalAreaId"))
        self.geographical_code = Master.process_null(elem.find("geographicalCode"))
        self.parent_geographical_area_group_sid = Master.process_null(elem.find("parentGeographicalAreaGroupSid"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        operation_date = g.app.get_timestamp()

        # Insert the geographical area
        sql = """
        insert into geographical_areas_oplog
        (hjid, geographical_area_sid, geographical_area_id, geographical_code, parent_geographical_area_group_sid,
        validity_start_date, validity_end_date,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = [
            self.hjid,
            self.geographical_area_sid,
            self.geographical_area_id,
            self.geographical_code,
            self.parent_geographical_area_group_sid,
            self.validity_start_date,
            self.validity_end_date,
            self.operation,
            operation_date,
            operation_date,
            import_file
        ]
        d = Database()
        d.run_query(sql, params)

        # Delete any geographical area descriptions
        sql = """
        delete from geographical_area_descriptions_oplog
        where geographical_area_sid = %s
        """
        params = [self.geographical_area_sid]
        d = Database()
        d.run_query(sql, params)

        # Delete any geographical area description periods
        sql = """
        delete from geographical_area_description_periods_oplog
        where geographical_area_sid = %s
        """
        params = [self.geographical_area_sid]
        d = Database()
        d.run_query(sql, params)

        # Delete any geographical area members
        sql = """
        delete from geographical_area_memberships_oplog
        where geographical_area_group_sid = %s
        """
        params = [self.geographical_area_sid]
        d = Database()
        d.run_query(sql, params)

        # Create new geographical area description periods
        for elem1 in elem.findall('.//geographicalAreaDescriptionPeriod'):
            GeographicalAreaDescriptionPeriod(elem1, self.geographical_area_sid, self.geographical_area_id, import_file)

        # Create new geographical area memberships
        for elem2 in elem.findall('.//geographicalAreaMembership'):
            GeographicalAreaMembership(elem2, self.geographical_area_sid, import_file)
