import common.globals as g


class profile_29505_prorogation_regulation_action(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        prorogation_regulation_role = app.get_value(omsg, ".//oub:prorogation.regulation.role", True)
        prorogation_regulation_id = app.get_value(omsg, ".//oub:prorogation.regulation.id", True)
        prorogated_regulation_role = app.get_value(omsg, ".//oub:prorogated.regulation.role", True)
        prorogated_regulation_id = app.get_value(omsg, ".//oub:prorogated.regulation.id", True)
        prorogated_date = app.get_date_value(omsg, ".//oub:prorogated.date", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "prorogation regulation action", prorogation_regulation_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO prorogation_regulation_actions_oplog (prorogation_regulation_role,
            prorogation_regulation_id, prorogated_regulation_role, prorogated_regulation_id, prorogated_date,
            operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (prorogation_regulation_role,
            prorogation_regulation_id, prorogated_regulation_role, prorogated_regulation_id, prorogated_date,
            operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, prorogation_regulation_id)
        cur.close()
