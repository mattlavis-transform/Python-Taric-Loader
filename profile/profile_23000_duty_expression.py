import common.globals as g


class profile_23000_duty_expression(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        duty_expression_id = app.get_value(omsg, ".//oub:duty.expression.id", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)
        duty_amount_applicability_code = app.get_number_value(omsg, ".//oub:duty.amount.applicability.code", True)
        measurement_unit_applicability_code = app.get_number_value(omsg, ".//oub:measurement.unit.applicability.code", True)
        monetary_unit_applicability_code = app.get_number_value(omsg, ".//oub:monetary.unit.applicability.code", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "duty expression", duty_expression_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO duty_expressions_oplog (duty_expression_id, validity_start_date,
            validity_end_date, duty_amount_applicability_code, measurement_unit_applicability_code, monetary_unit_applicability_code,
            operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (duty_expression_id, validity_start_date, validity_end_date,
            duty_amount_applicability_code, measurement_unit_applicability_code, monetary_unit_applicability_code,
            operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, duty_expression_id)
        cur.close()
