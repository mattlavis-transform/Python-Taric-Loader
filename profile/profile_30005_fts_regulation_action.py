import common.globals as g


class profile_30005_fts_regulation_action(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        fts_regulation_role = app.get_value(omsg, ".//oub:fts.regulation.role", True)
        fts_regulation_id = app.get_value(omsg, ".//oub:fts.regulation.id", True)
        stopped_regulation_role = app.get_value(omsg, ".//oub:stopped.regulation.role", True)
        stopped_regulation_id = app.get_value(omsg, ".//oub:stopped.regulation.id", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "certifiFTS regulation action", fts_regulation_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO fts_regulation_actions_oplog (fts_regulation_role, fts_regulation_id,
            stopped_regulation_role, stopped_regulation_id,
            operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (fts_regulation_role, fts_regulation_id, stopped_regulation_role, stopped_regulation_id,
            operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, fts_regulation_id)
        cur.close()
