import common.globals as g


class profile_23005_duty_expression_description(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        duty_expression_id = app.get_value(omsg, ".//oub:duty.expression.id", True)
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        description = app.get_value(omsg, ".//oub:description", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "duty expression description", duty_expression_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO duty_expression_descriptions_oplog (duty_expression_id, language_id, description, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (duty_expression_id, language_id, description, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, duty_expression_id)
        cur.close()
