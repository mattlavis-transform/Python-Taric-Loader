import common.globals as g


class profile_34000_meursing_additional_code(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = g.app.get_timestamp()
        meursing_additional_code_sid = g.app.get_number_value(omsg, ".//oub:meursing.additional.code.sid", True)
        validity_end_date = g.app.get_date_value(omsg, ".//oub:validity.end.date", True)
        additional_code = g.app.get_value(omsg, ".//oub:additional.code", True)
        validity_start_date = g.app.get_date_value(omsg, ".//oub:validity.start.date", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "Meursing additional code", additional_code)

        # Load data
        cur = g.app.conn.cursor()
        try:
            cur.execute("""INSERT INTO meursing_additional_codes_oplog (meursing_additional_code_sid,
            validity_end_date, additional_code, validity_start_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (meursing_additional_code_sid,
            validity_end_date, additional_code, validity_start_date, operation, operation_date))
            g.app.conn.commit()
        except:
            g.data_file.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, additional_code)
        cur.close()
