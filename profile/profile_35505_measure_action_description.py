import common.globals as g


class profile_35505_measure_action_description(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        action_code = app.get_value(omsg, ".//oub:action.code", True)
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        description = app.get_value(omsg, ".//oub:description", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "measure action description", action_code)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO measure_action_descriptions_oplog (action_code, language_id, description, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (action_code, language_id, description, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, action_code)
        cur.close()
