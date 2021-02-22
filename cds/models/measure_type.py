import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.measure_type_description import MeasureTypeDescription


class MeasureType(Master):
    def __init__(self, elem, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.measure_type_id = Master.process_null(elem.find("measureTypeId"))
        self.measure_component_applicable_code = Master.process_null(elem.find("measureComponentApplicableCode"))
        self.measure_explosion_level = Master.process_null(elem.find("measureExplosionLevel"))
        self.order_number_capture_code = Master.process_null(elem.find("orderNumberCaptureCode"))
        self.origin_dest_code = Master.process_null(elem.find("originDestCode"))
        self.priority_code = Master.process_null(elem.find("priorityCode"))
        self.trade_movement_code = Master.process_null(elem.find("tradeMovementCode"))
        self.measure_type_series_id = Master.process_null(elem.find("measureTypeSeries/measureTypeSeriesId"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        self.application_code = Master.process_null(elem.find("applicationCode"))
        operation_date = g.app.get_timestamp()

        # Insert the measure type
        if transform_only is False:
            sql = """
            insert into measure_types_oplog
            (measure_type_id, validity_start_date, validity_end_date,
            measure_component_applicable_code, measure_explosion_level, order_number_capture_code,
            origin_dest_code, priority_code, trade_movement_code, measure_type_series_id,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.measure_type_id,
                self.validity_start_date,
                self.validity_end_date,
                self.measure_component_applicable_code,
                self.measure_explosion_level,
                self.order_number_capture_code,
                self.origin_dest_code,
                self.priority_code,
                self.trade_movement_code,
                self.measure_type_series_id,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)

            # Delete any measure descriptions
            sql = """
            delete from measure_type_descriptions_oplog
            where measure_type_id = %s
            """
            params = [self.measure_type_id]
            d = Database()
            d.run_query(sql, params)

            # Create new measure type descriptions
            for elem in elem.findall('.//measureTypeDescription'):
                MeasureTypeDescription(elem, self.measure_type_id, import_file)
