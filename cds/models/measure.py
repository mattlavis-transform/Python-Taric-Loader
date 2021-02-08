from cds.models.goods_nomenclature_indent import GoodsNomenclatureIndent
import sys

import common.globals as g
from common.database import Database
from cds.models.master import Master
from cds.models.measure_excluded_geographical_area import MeasureExcludedGeographicalArea
from cds.models.measure_component import MeasureComponent
from cds.models.measure_condition import MeasureCondition
from cds.models.footnote_association_measure import FootnoteAssociationMeasure


class Measure(Master):
    def __init__(self, elem, import_file):
        Master.__init__(self, elem)
        self.measure_sid = Master.process_null(elem.find("sid"))
        self.measure_type_id = Master.process_null(elem.find("measureType/measureTypeId"))
        self.geographical_area_id = Master.process_null(elem.find("geographicalArea/geographicalAreaId"))
        self.goods_nomenclature_item_id = Master.process_null(elem.find("goodsNomenclature/goodsNomenclatureItemId"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        self.measure_generating_regulation_role = Master.process_null(elem.find("measureGeneratingRegulationRole/regulationRoleTypeId"))
        self.measure_generating_regulation_id = Master.process_null(elem.find("measureGeneratingRegulationId"))
        self.justification_regulation_id = Master.process_null(elem.find("justificationRegulationId"))
        self.justification_regulation_role = Master.process_null(elem.find("justificationRegulationRole/regulationRoleTypeId"))
        self.stopped_flag = Master.process_null(elem.find("stoppedFlag"))
        self.geographical_area_sid = Master.process_null(elem.find("geographicalArea/sid"))
        self.goods_nomenclature_sid = Master.process_null(elem.find("goodsNomenclature/sid"))
        self.ordernumber = Master.process_null(elem.find("ordernumber"))
        self.additional_code_type_id = Master.process_null(elem.find("additionalCode/additionalCodeType/additionalCodeTypeId"))
        self.additional_code_id = Master.process_null(elem.find("additionalCode/additionalCodeCode"))
        self.additional_code_sid = Master.process_null(elem.find("additionalCode/sid"))
        self.reduction_indicator = Master.process_null(elem.find("reductionIndicator"))
        self.export_refund_nomenclature_sid = Master.process_null(elem.find("exportRefundNomenclatureSid"))

        operation_date = g.app.get_timestamp()

        # Insert the goods nomenclature
        sql = """
        insert into measures_oplog
        (
            measure_sid, measure_type_id, geographical_area_id,
            goods_nomenclature_item_id, validity_start_date, validity_end_date,
            measure_generating_regulation_role, measure_generating_regulation_id,
            justification_regulation_role, justification_regulation_id,
            stopped_flag, geographical_area_sid, goods_nomenclature_sid,
            ordernumber, additional_code_type_id, additional_code_id,
            additional_code_sid, reduction_indicator, export_refund_nomenclature_sid,
            operation, operation_date, created_at, filename
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = [
            self.measure_sid,
            self.measure_type_id,
            self.geographical_area_id,
            self.goods_nomenclature_item_id,
            self.validity_start_date,
            self.validity_end_date,
            self.measure_generating_regulation_role,
            self.measure_generating_regulation_id,
            self.justification_regulation_role,
            self.justification_regulation_id,
            self.stopped_flag,
            self.geographical_area_sid,
            self.goods_nomenclature_sid,
            self.ordernumber,
            self.additional_code_type_id,
            self.additional_code_id,
            self.additional_code_sid,
            self.reduction_indicator,
            self.export_refund_nomenclature_sid,
            self.operation,
            operation_date,
            operation_date,
            import_file
        ]
        d = Database()
        d.run_query(sql, params)

        # Delete any measure excluded geographical areas
        sql = """delete from measure_excluded_geographical_areas_oplog where measure_sid = %s"""
        params = [self.measure_sid]
        d = Database()
        d.run_query(sql, params)

        # Delete any measure components
        sql = """delete from measure_components_oplog where measure_sid = %s"""
        params = [self.measure_sid]
        d = Database()
        d.run_query(sql, params)

        # Delete any measure conditions
        sql = """delete from measure_conditions_oplog where measure_sid = %s"""
        params = [self.measure_sid]
        d = Database()
        d.run_query(sql, params)

        # Delete any footnote associations
        sql = """delete from footnote_association_measures_oplog where measure_sid = %s"""
        params = [self.measure_sid]
        d = Database()
        d.run_query(sql, params)

        # Create new measureExcludedGeographicalArea
        for elem1 in elem.findall('.//measureExcludedGeographicalArea'):
            MeasureExcludedGeographicalArea(elem1, self.measure_sid, import_file)

        # Create new measure components
        for elem2 in elem.findall('.//measureComponent'):
            MeasureComponent(elem2, self.measure_sid, import_file)

        # Create new measure conditions
        for elem3 in elem.findall('.//measureCondition'):
            MeasureCondition(elem3, self.measure_sid, import_file)

        # Create new footnote associations to measures
        for elem4 in elem.findall('.//footnoteAssociationMeasure'):
            FootnoteAssociationMeasure(elem4, self.measure_sid, import_file)
