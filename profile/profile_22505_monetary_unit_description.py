import common.globals as g


class profile_22505_monetary_unit_description(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        monetary_unit_code = app.get_value(omsg, ".//oub:monetary.unit.code", True)
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        description = app.get_value(omsg, ".//oub:description", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "monetary unit description", monetary_unit_code)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO monetary_unit_descriptions_oplog (monetary_unit_code, language_id, description, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (monetary_unit_code, language_id, description, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, monetary_unit_code)
        cur.close()
