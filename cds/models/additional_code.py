import sys

import common.globals as g
from common.database import Database
from cds.models.master import Master
from cds.models.additional_code_description_period import AdditionalCodeDescriptionPeriod


class AdditionalCode(Master):
    def __init__(self, elem, import_file):
        Master.__init__(self, elem)
        self.additional_code_sid = Master.process_null(elem.find("sid"))
        self.additional_code_type_id = Master.process_null(elem.find("additionalCodeType/additionalCodeTypeId"))
        self.additional_code = Master.process_null(elem.find("additionalCodeCode"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        operation_date = g.app.get_timestamp()

        # Insert the additional code
        sql = """
        insert into additional_codes_oplog
        (additional_code_sid, additional_code_type_id, additional_code, validity_start_date, validity_end_date,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = [
            self.additional_code_sid,
            self.additional_code_type_id,
            self.additional_code,
            self.validity_start_date,
            self.validity_end_date,
            self.operation,
            operation_date,
            operation_date,
            import_file
        ]
        d = Database()
        d.run_query(sql, params)

        # Delete any additional code descriptions
        sql = """
        delete from additional_code_descriptions_oplog
        where additional_code_sid = %s
        """
        params = [self.additional_code_sid]
        d = Database()
        d.run_query(sql, params)

        # Delete any additional code description periods
        sql = """
        delete from additional_code_description_periods_oplog
        where additional_code_sid = %s
        """
        params = [self.additional_code_sid]
        d = Database()
        d.run_query(sql, params)

        # Create new additional code description periods
        for elem in elem.findall('.//additionalCodeDescriptionPeriod'):
            AdditionalCodeDescriptionPeriod(elem, self.additional_code_sid, self.additional_code_type_id, self.additional_code, import_file)
