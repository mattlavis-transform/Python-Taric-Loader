import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.additional_code_type_description import AdditionalCodeTypeDescription
from cds.models.additional_code_type_measure_type import AdditionalCodeTypeMeasureType


class AdditionalCodeType(Master):
    def __init__(self, elem, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.additional_code_type_id = Master.process_null(elem.find("additionalCodeTypeId"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        self.application_code = Master.process_null(elem.find("applicationCode"))
        self.meursing_table_plan_id = Master.process_null(elem.find("meursingTablePlan/meursingTablePlanId"))
        operation_date = g.app.get_timestamp()

        g.app.print_only("Running operation {0} on additional code type {1}.".format(self.operation, str(self.additional_code_type_id)))

        # Insert the additional_code type
        if transform_only is False:
            sql = """
            insert into additional_code_types_oplog
            (additional_code_type_id, application_code, meursing_table_plan_id,
            validity_start_date, validity_end_date,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.additional_code_type_id,
                self.application_code,
                self.meursing_table_plan_id,
                self.validity_start_date,
                self.validity_end_date,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)

            # Delete any additional_code descriptions
            sql = """
            delete from additional_code_type_descriptions_oplog
            where additional_code_type_id = %s
            """
            params = [self.additional_code_type_id]
            d = Database()
            d.run_query(sql, params)

            # Delete any additional_code measure_types
            sql = """
            delete from additional_code_type_measure_types_oplog
            where additional_code_type_id = %s
            """
            params = [self.additional_code_type_id]
            d = Database()
            d.run_query(sql, params)

            # Create new additional_code type descriptions
            for elem2 in elem.findall('.//additionalCodeTypeDescription'):
                AdditionalCodeTypeDescription(elem2, self.additional_code_type_id, import_file)

            # Create new additional_code type measure types
            for elem3 in elem.findall('.//additionalCodeTypeMeasureType'):
                AdditionalCodeTypeMeasureType(elem3, self.additional_code_type_id, import_file)
