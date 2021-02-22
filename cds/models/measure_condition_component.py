import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master


class MeasureConditionComponent(Master):
    def __init__(self, elem, measure_condition_sid, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.measure_condition_sid = measure_condition_sid
        self.duty_expression_id = Master.process_null(elem.find("dutyExpression/dutyExpressionId"))
        self.duty_amount = Master.process_null(elem.find("dutyAmount"))
        self.monetary_unit_code = Master.process_null(elem.find("monetaryUnit/monetaryUnitCode"))
        self.measurement_unit_code = Master.process_null(elem.find("measurementUnit/measurementUnitCode"))
        self.measurement_unit_qualifier_code = Master.process_null(elem.find("measurementUnitQualifier/measurementUnitQualifierCode"))

        operation_date = g.app.get_timestamp()

        # Insert the MeasureComponent
        if transform_only is False:
            sql = """
            insert into measure_condition_components_oplog
            (measure_condition_sid, duty_expression_id, duty_amount,
            monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.measure_condition_sid,
                self.duty_expression_id,
                self.duty_amount,
                self.monetary_unit_code,
                self.measurement_unit_code,
                self.measurement_unit_qualifier_code,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)
