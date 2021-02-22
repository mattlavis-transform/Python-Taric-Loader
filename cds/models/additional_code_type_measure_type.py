import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master


class AdditionalCodeTypeMeasureType(Master):
    def __init__(self, elem, additional_code_type_id, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.additional_code_type_id = additional_code_type_id
        self.measure_type_id = Master.process_null(elem.find("measureType/measureTypeId"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        operation_date = g.app.get_timestamp()

        g.app.print_only("Running operation {0} on additional code type measure type {1}.".format(self.operation, self.additional_code_type_id))

        if transform_only is False:
            sql = """
            insert into additional_code_type_measure_types_oplog
            (additional_code_type_id, measure_type_id,
            validity_start_date, validity_end_date,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.additional_code_type_id,
                self.measure_type_id,
                self.validity_start_date,
                self.validity_end_date,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)
