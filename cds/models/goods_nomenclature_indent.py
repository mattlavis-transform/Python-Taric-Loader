import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.goods_nomenclature_description import GoodsNomenclatureDescription


class GoodsNomenclatureIndent(Master):
    def __init__(self, elem, goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.goods_nomenclature_sid = goods_nomenclature_sid
        self.goods_nomenclature_item_id = goods_nomenclature_item_id
        self.productline_suffix = productline_suffix
        self.goods_nomenclature_indent_sid = Master.process_null(elem.find("sid"))
        self.number_indents = Master.process_null(elem.find("numberIndents"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))

        operation_date = g.app.get_timestamp()

        # Insert the footnote description period
        if transform_only is False:
            sql = """
            insert into goods_nomenclature_indents_oplog
            (goods_nomenclature_indent_sid, goods_nomenclature_sid, validity_start_date,
            number_indents, goods_nomenclature_item_id, productline_suffix, validity_end_date,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.goods_nomenclature_indent_sid,
                self.goods_nomenclature_sid,
                self.validity_start_date,
                self.number_indents,
                self.goods_nomenclature_item_id,
                self.productline_suffix,
                self.validity_end_date,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)
