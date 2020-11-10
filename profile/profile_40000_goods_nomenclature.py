from datetime import datetime
import common.globals as g


class profile_40000_goods_nomenclature(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        goods_nomenclature_sid = app.get_number_value(omsg, ".//oub:goods.nomenclature.sid", True)
        goods_nomenclature_item_id = app.get_value(omsg, ".//oub:goods.nomenclature.item.id", True)
        producline_suffix = app.get_value(omsg, ".//oub:producline.suffix", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)
        statistical_indicator = app.get_value(omsg, ".//oub:statistical.indicator", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "goods nomenclature", goods_nomenclature_sid)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO goods_nomenclatures_oplog (goods_nomenclature_sid,
            goods_nomenclature_item_id, producline_suffix,
            validity_start_date, validity_end_date, statistical_indicator, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (goods_nomenclature_sid,
            goods_nomenclature_item_id, producline_suffix,
            validity_start_date, validity_end_date, statistical_indicator, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, goods_nomenclature_item_id)
        cur.close()

        g.app.goods_nomenclatures = g.app.get_all_goods_nomenclatures()
