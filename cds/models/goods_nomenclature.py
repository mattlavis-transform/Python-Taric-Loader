from cds.models.goods_nomenclature_indent import GoodsNomenclatureIndent
import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.footnote_association_goods_nomenclature import FootnoteAssociationGoodsNomenclature
from cds.models.goods_nomenclature_description_period import GoodsNomenclatureDescriptionPeriod


class GoodsNomenclature(Master):
    def __init__(self, elem, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.goods_nomenclature_sid = Master.process_null(elem.find("sid"))
        self.goods_nomenclature_item_id = Master.process_null(elem.find("goodsNomenclatureItemId"))
        self.producline_suffix = Master.process_null(elem.find("produclineSuffix"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        self.statistical_indicator = Master.process_null(elem.find("statisticalIndicator"))
        operation_date = g.app.get_timestamp()

        # Insert the goods nomenclature
        if transform_only is False:
            sql = """
            insert into goods_nomenclatures_oplog
            (goods_nomenclature_sid, goods_nomenclature_item_id, producline_suffix,
            validity_start_date, validity_end_date, statistical_indicator,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.goods_nomenclature_sid,
                self.goods_nomenclature_item_id,
                self.producline_suffix,
                self.validity_start_date,
                self.validity_end_date,
                self.statistical_indicator,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)

            # Delete any footnote Association GoodsNomenclatures
            sql = """
            delete from footnote_association_goods_nomenclatures_oplog
            where goods_nomenclature_sid = %s
            """
            params = [self.goods_nomenclature_sid]
            d = Database()
            d.run_query(sql, params)

            # Delete any goodsNomenclatureDescriptionPeriod
            sql = """
            delete from goods_nomenclature_description_periods_oplog
            where goods_nomenclature_sid = %s
            """
            params = [self.goods_nomenclature_sid]
            d = Database()
            d.run_query(sql, params)

            # Create new quota order number origins
            for elem1 in elem.findall('.//footnoteAssociationGoodsNomenclature'):
                FootnoteAssociationGoodsNomenclature(elem1, self.goods_nomenclature_sid, self.goods_nomenclature_item_id, self.producline_suffix, import_file)

            # Create new quota goods Nomenclature Description Periods
            for elem2 in elem.findall('.//goodsNomenclatureDescriptionPeriod'):
                GoodsNomenclatureDescriptionPeriod(elem2, self.goods_nomenclature_sid, self.goods_nomenclature_item_id, self.producline_suffix, import_file)

            # goodsNomenclatureIndents
            for elem3 in elem.findall('.//goodsNomenclatureIndents'):
                GoodsNomenclatureIndent(elem3, self.goods_nomenclature_sid, self.goods_nomenclature_item_id, self.producline_suffix, import_file)
