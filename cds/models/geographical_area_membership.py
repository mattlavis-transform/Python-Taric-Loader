import common.globals as g
from common.database import Database
from cds.models.master import Master


class GeographicalAreaMembership(Master):
    def __init__(self, elem, geographical_area_group_sid, import_file):
        Master.__init__(self, elem)
        # the inputs are:
        # validityStartDate
        # validityEndDate
        # geographicalAreaGroupSid, which actually means the hjid of the child

        self.geographical_area_sid = None
        self.geographical_area_group_sid = geographical_area_group_sid
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        self.child_hjid = Master.process_null(elem.find("geographicalAreaGroupSid"))
        self.lookup_child_hjid()

        operation_date = g.app.get_timestamp()
        sql = """
        insert into geographical_area_memberships_oplog
        (geographical_area_sid, geographical_area_group_sid,
        validity_start_date, validity_end_date,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = [
            self.geographical_area_sid,
            self.geographical_area_group_sid,
            self.validity_start_date,
            self.validity_end_date,
            self.operation,
            operation_date,
            operation_date,
            import_file
        ]
        d = Database()
        d.run_query(sql, params)

    def lookup_child_hjid(self):
        sql = """
        select geographical_area_sid from geographical_areas_oplog where hjid = %s
        """
        params = [
            self.child_hjid
        ]
        d = Database()
        rows = d.run_query(sql, params)
        if len(rows) > 0:
            row = rows[0]
            self.geographical_area_sid = row[0]
