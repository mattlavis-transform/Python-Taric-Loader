import common.globals as g


class profile_41000_export_refund_nomenclature(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        export_refund_nomenclature_sid = app.get_number_value(omsg, ".//oub:export.refund.nomenclature.sid", True)
        goods_nomenclature_item_id = app.get_value(omsg, ".//oub:goods.nomenclature.item.id", True)
        additional_code_type = app.get_value(omsg, ".//oub:additional.code.type", True)
        export_refund_code = app.get_value(omsg, ".//oub:export.refund.code", True)
        productline_suffix = app.get_value(omsg, ".//oub:productline.suffix", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)
        goods_nomenclature_sid = app.get_value(omsg, ".//oub:goods.nomenclature.sid", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "export refund nomenclature", export_refund_nomenclature_sid)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO export_refund_nomenclatures_oplog (export_refund_nomenclature_sid,
            goods_nomenclature_item_id, additional_code_type, export_refund_code, productline_suffix,
            validity_start_date, validity_end_date, goods_nomenclature_sid, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (export_refund_nomenclature_sid,
            goods_nomenclature_item_id, additional_code_type, export_refund_code, productline_suffix,
            validity_start_date, validity_end_date, goods_nomenclature_sid, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, export_refund_nomenclature_sid)
        cur.close()

        g.app.goods_nomenclatures = g.app.get_all_goods_nomenclatures()
