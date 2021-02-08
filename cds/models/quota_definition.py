import sys

import common.globals as g
from common.database import Database
from cds.models.master import Master
from cds.models.quota_association import QuotaAssociation
from cds.models.quota_blocking_period import QuotaBlockingPeriod
from cds.models.quota_suspension_period import QuotaSuspensionPeriod
from cds.models.quota_balance_event import QuotaBalanceEvent


class QuotaDefinition(Master):
    def __init__(self, elem, import_file):
        Master.__init__(self, elem)

        self.quota_definition_sid = Master.process_null(elem.find("sid"))
        self.quota_order_number_id = Master.process_null(elem.find("quotaOrderNumber/quotaOrderNumberId"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        self.quota_order_number_sid = Master.process_null(elem.find("quotaOrderNumber/sid"))
        self.volume = Master.process_null(elem.find("volume"))
        self.initial_volume = Master.process_null(elem.find("initialVolume"))
        self.measurement_unit_code = Master.process_null(elem.find("measurementUnit/measurementUnitCode"))
        self.maximum_precision = Master.process_null(elem.find("maximumPrecision"))
        self.critical_state = Master.process_null(elem.find("criticalState"))
        self.critical_threshold = Master.process_null(elem.find("criticalThreshold"))
        self.monetary_unit_code = Master.process_null(elem.find("monetaryUnit/monetaryUnitCode"))
        self.measurement_unit_qualifier_code = Master.process_null(elem.find("measurementUnitQualifier/measurementUnitQualifierCode"))
        self.description = Master.process_null(elem.find("description"))

        operation_date = g.app.get_timestamp()

        # Insert the quota order number
        sql = """
        insert into quota_definitions_oplog
        (quota_definition_sid, quota_order_number_id,
        validity_start_date, validity_end_date,
        quota_order_number_sid, volume, initial_volume,
        measurement_unit_code, maximum_precision, critical_state,
        critical_threshold, monetary_unit_code,
        measurement_unit_qualifier_code, description,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = [
            self.quota_definition_sid,
            self.quota_order_number_id,
            self.validity_start_date,
            self.validity_end_date,
            self.quota_order_number_sid,
            self.volume,
            self.initial_volume,
            self.measurement_unit_code,
            self.maximum_precision,
            self.critical_state,
            self.critical_threshold,
            self.monetary_unit_code,
            self.measurement_unit_qualifier_code,
            self.description,
            self.operation,
            operation_date,
            operation_date,
            import_file
        ]
        d = Database()
        d.run_query(sql, params)

        # Delete any quota associations
        sql = "delete from quota_associations_oplog where main_quota_definition_sid = %s"
        params = [self.quota_definition_sid]
        d = Database()
        d.run_query(sql, params)

        # Delete any quota blocking periods
        sql = "delete from quota_blocking_periods_oplog where quota_definition_sid = %s"
        params = [self.quota_definition_sid]
        d = Database()
        d.run_query(sql, params)

        # Delete any quota suspension periods
        sql = "delete from quota_suspension_periods_oplog where quota_definition_sid = %s"
        params = [self.quota_definition_sid]
        d = Database()
        d.run_query(sql, params)

        # Delete any quota balance events
        sql = "delete from quota_balance_events_oplog where quota_definition_sid = %s"
        params = [self.quota_definition_sid]
        d = Database()
        d.run_query(sql, params)

        # Create new quota associations
        for elem1 in elem.findall('.//quotaAssociation'):
            QuotaAssociation(elem1, self.quota_definition_sid, import_file)

        # Create new quota blocking periods
        for elem2 in elem.findall('.//quotaBlockingPeriod'):
            QuotaBlockingPeriod(elem2, self.quota_definition_sid, import_file)

        # Create new quota suspension periods
        for elem3 in elem.findall('.//quotaSuspensionPeriod'):
            QuotaSuspensionPeriod(elem3, self.quota_definition_sid, import_file)

        # Create new quota balance events
        for elem4 in elem.findall('.//quotaBalanceEvent'):
            QuotaBalanceEvent(elem4, self.quota_definition_sid, import_file)
