import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master


class FootnoteAssociationGoodsNomenclature(Master):
    def __init__(self, elem, goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.goods_nomenclature_sid = goods_nomenclature_sid
        self.goods_nomenclature_item_id = goods_nomenclature_item_id
        self.productline_suffix = productline_suffix
        self.footnote_type_id = Master.process_null(elem.find("footnote/footnoteType/footnoteTypeId"))
        self.footnote_id = Master.process_null(elem.find("footnote/footnoteId"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))

        operation_date = g.app.get_timestamp()
        if transform_only is False:
            sql = """
            insert into footnote_association_goods_nomenclatures_oplog
            (goods_nomenclature_sid, footnote_type, footnote_id,
            validity_start_date, validity_end_date, goods_nomenclature_item_id, productline_suffix,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.goods_nomenclature_sid,
                self.footnote_type_id,
                self.footnote_id,
                self.validity_start_date,
                self.validity_end_date,
                self.goods_nomenclature_item_id,
                self.productline_suffix,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)
