import common.globals as g


class profile_24000_additional_code_type_measure_type(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = g.app.get_timestamp()
        measure_type_id = g.app.get_value(omsg, ".//oub:measure.type.id", True)
        additional_code_type_id = g.app.get_value(omsg, ".//oub:additional.code.type.id", True)
        validity_start_date = g.app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = g.app.get_date_value(omsg, ".//oub:validity.end.date", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "additional code type / measure type", measure_type_id + "/" + additional_code_type_id)

        # Load data
        cur = g.app.conn.cursor()
        try:
            cur.execute("""INSERT INTO additional_code_type_measure_types_oplog (measure_type_id, additional_code_type_id,
            validity_start_date, validity_end_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (measure_type_id, additional_code_type_id, validity_start_date, validity_end_date, operation, operation_date))
            g.app.conn.commit()
        except:
            g.data_file.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, measure_type_id)
        cur.close()
