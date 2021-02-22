import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.measure_condition_component import MeasureConditionComponent


class MeasureCondition(Master):
    def __init__(self, elem, measure_sid, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.measure_sid = measure_sid
        self.measure_condition_sid = Master.process_null(elem.find("sid"))
        self.condition_code = Master.process_null(elem.find("measureConditionCode/conditionCode"))
        self.component_sequence_number = Master.process_null(elem.find("conditionSequenceNumber"))
        self.condition_duty_amount = Master.process_null(elem.find("conditionDutyAmount"))
        self.condition_monetary_unit_code = Master.process_null(elem.find("monetaryUnit/monetaryUnitCode"))
        self.condition_measurement_unit_code = Master.process_null(elem.find("measurementUnit/measurementUnitCode"))
        self.condition_measurement_unit_qualifier_code = Master.process_null(elem.find("measurementUnitQualifier/measurementUnitQualifierCode"))
        self.action_code = Master.process_null(elem.find("measureAction/actionCode"))
        self.certificate_type_code = Master.process_null(elem.find("certificate/certificateType/certificateTypeCode"))
        self.certificate_code = Master.process_null(elem.find("certificate/certificateCode"))

        operation_date = g.app.get_timestamp()

        # Insert the measure condition
        if transform_only is False:
            sql = """
            insert into measure_conditions_oplog
            (measure_sid, measure_condition_sid, condition_code,
            component_sequence_number, condition_duty_amount, condition_monetary_unit_code,
            condition_measurement_unit_qualifier_code, action_code, certificate_type_code, certificate_code,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.measure_sid,
                self.measure_condition_sid,
                self.condition_code,
                self.component_sequence_number,
                self.condition_duty_amount,
                self.condition_monetary_unit_code,
                self.condition_measurement_unit_qualifier_code,
                self.action_code,
                self.certificate_type_code,
                self.certificate_code,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)

            # Delete any measure condition components
            sql = """delete from measure_condition_components_oplog where measure_condition_sid = %s"""
            params = [self.measure_condition_sid]
            d = Database()
            d.run_query(sql, params)

            # Create new measure condition components
            for elem1 in elem.findall('.//measureConditionComponent'):
                MeasureConditionComponent(elem1, self.measure_condition_sid, import_file)
