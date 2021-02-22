import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master


class GoodsNomenclatureDescription(Master):
    def __init__(self, elem, goods_nomenclature_description_period_sid, goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.goods_nomenclature_description_period_sid = goods_nomenclature_description_period_sid
        self.goods_nomenclature_sid = goods_nomenclature_sid
        self.goods_nomenclature_item_id = goods_nomenclature_item_id
        self.productline_suffix = productline_suffix
        self.description = Master.process_null(elem.find("description"))
        self.language_id = Master.process_null(elem.find("language/languageId"))
        operation_date = g.app.get_timestamp()

        if transform_only is False:
            sql = """
            insert into goods_nomenclature_descriptions_oplog
            (goods_nomenclature_description_period_sid, goods_nomenclature_sid,
            goods_nomenclature_item_id, productline_suffix, language_id, description,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.goods_nomenclature_description_period_sid,
                self.goods_nomenclature_sid,
                self.goods_nomenclature_item_id,
                self.productline_suffix,
                self.language_id,
                self.description,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)
