import common.globals as g


class profile_13005_language_description(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = g.app.get_timestamp()
        language_code_id = g.app.get_value(omsg, ".//oub:language.code.id", True)
        language_id = g.app.get_value(omsg, ".//oub:language.id", True)
        description = g.app.get_value(omsg, ".//oub:description", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "language description", language_id)

        # Load data
        cur = g.app.conn.cursor()
        try:
            cur.execute("""INSERT INTO language_descriptions_oplog (language_code_id, language_id, description, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (language_code_id, language_id, description, operation, operation_date))
            g.app.conn.commit()
        except:
            g.data_file.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, language_id)
        cur.close()
