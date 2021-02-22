import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.goods_nomenclature_description import GoodsNomenclatureDescription


class GoodsNomenclatureDescriptionPeriod(Master):
    def __init__(self, elem, goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.goods_nomenclature_sid = goods_nomenclature_sid
        self.goods_nomenclature_item_id = goods_nomenclature_item_id
        self.productline_suffix = productline_suffix
        self.goods_nomenclature_description_period_sid = Master.process_null(elem.find("sid"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        operation_date = g.app.get_timestamp()

        # Insert the footnote description period
        if transform_only is False:
            sql = """
            insert into goods_nomenclature_description_periods_oplog
            (goods_nomenclature_description_period_sid, goods_nomenclature_sid, validity_start_date,
            goods_nomenclature_item_id, productline_suffix,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.goods_nomenclature_description_period_sid,
                self.goods_nomenclature_sid,
                self.validity_start_date,
                self.goods_nomenclature_item_id,
                self.productline_suffix,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)

            # Create new goods nomenclature descriptions
            for elem in elem.findall('.//goodsNomenclatureDescription'):
                GoodsNomenclatureDescription(elem, self.goods_nomenclature_description_period_sid, self.goods_nomenclature_sid, self.goods_nomenclature_item_id, self.productline_suffix, import_file)
