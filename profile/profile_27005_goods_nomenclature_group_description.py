import sys
import common.globals as g


class profile_27005_goods_nomenclature_group_description(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        goods_nomenclature_group_type = app.get_value(omsg, ".//oub:goods.nomenclature.group.type", True)
        goods_nomenclature_group_id = app.get_value(omsg, ".//oub:goods.nomenclature.group.id", True)
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        description = app.get_value(omsg, ".//oub:description", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "goods nomenclature group description", goods_nomenclature_group_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO goods_nomenclature_group_descriptions_oplog (goods_nomenclature_group_type, goods_nomenclature_group_id,
            language_id, description,
            operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (goods_nomenclature_group_type, goods_nomenclature_group_id,
            language_id, description,
            operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, goods_nomenclature_group_id)
        cur.close()
