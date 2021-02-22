import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.additional_code_description import AdditionalCodeDescription


class AdditionalCodeDescriptionPeriod(Master):
    def __init__(self, elem, additional_code_sid, additional_code_type_id, additional_code, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.additional_code_sid = additional_code_sid
        self.additional_code_type_id = additional_code_type_id
        self.additional_code = additional_code
        self.additional_code_description_period_sid = Master.process_null(elem.find("sid"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        operation_date = g.app.get_timestamp()

        g.app.print_only("Running operation {0} on additional code description period {1}.".format(self.operation, self.additional_code_description_period_sid))

        # Insert the additional code description period
        if transform_only is False:
            sql = """
            insert into additional_code_description_periods_oplog
            (additional_code_description_period_sid, additional_code_sid, additional_code_type_id,
            additional_code, validity_start_date, validity_end_date,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.additional_code_description_period_sid,
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

            # Create new additional code descriptions
            for elem in elem.findall('.//additionalCodeDescription'):
                AdditionalCodeDescription(elem, self.additional_code_description_period_sid, self.additional_code_sid, self.additional_code_type_id, self.additional_code, import_file)
