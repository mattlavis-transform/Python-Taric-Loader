import common.globals as g


class profile_40035_goods_nomenclature_origin(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = g.app.get_timestamp()
        goods_nomenclature_sid = g.app.get_number_value(omsg, ".//oub:goods.nomenclature.sid", True)
        derived_goods_nomenclature_item_id = g.app.get_value(omsg, ".//oub:derived.goods.nomenclature.item.id", True)
        derived_productline_suffix = g.app.get_value(omsg, ".//oub:derived.productline.suffix", True)
        goods_nomenclature_item_id = g.app.get_value(omsg, ".//oub:goods.nomenclature.item.id", True)
        productline_suffix = g.app.get_value(omsg, ".//oub:productline.suffix", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "goods nomenclature origin", goods_nomenclature_sid)

        # Load data
        cur = g.app.conn.cursor()
        try:
            cur.execute("""INSERT INTO goods_nomenclature_origins_oplog (goods_nomenclature_sid,
            derived_goods_nomenclature_item_id, derived_productline_suffix,
            goods_nomenclature_item_id, productline_suffix,
            operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (goods_nomenclature_sid,
            derived_goods_nomenclature_item_id, derived_productline_suffix,
            goods_nomenclature_item_id, productline_suffix,
            operation, operation_date))
            g.app.conn.commit()
        except:
            g.data_file.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, goods_nomenclature_item_id)
        cur.close()
