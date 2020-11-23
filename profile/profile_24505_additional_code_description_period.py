import common.globals as g


class profile_24505_additional_code_description_period(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = g.app.get_timestamp()
        additional_code_description_period_sid = g.app.get_number_value(omsg, ".//oub:additional.code.description.period.sid", True)
        additional_code_sid = g.app.get_number_value(omsg, ".//oub:additional.code.sid", True)
        additional_code_type_id = g.app.get_value(omsg, ".//oub:additional.code.type.id", True)
        additional_code = g.app.get_value(omsg, ".//oub:additional.code", True)
        code = additional_code_type_id + additional_code
        validity_start_date = g.app.get_date_value(omsg, ".//oub:validity.start.date", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "additional code description period", additional_code_description_period_sid)

        # Load data
        cur = g.app.conn.cursor()
        try:
            cur.execute("""INSERT INTO additional_code_description_periods_oplog (additional_code_description_period_sid,
            additional_code_sid, additional_code_type_id, additional_code, validity_start_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (additional_code_description_period_sid, additional_code_sid, additional_code_type_id, additional_code, validity_start_date, operation, operation_date))
            g.app.conn.commit()
        except:
            g.data_file.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, code)
        cur.close()
