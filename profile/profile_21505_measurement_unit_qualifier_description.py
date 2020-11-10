import common.globals as g


class profile_21505_measurement_unit_qualifier_description(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        measurement_unit_qualifier_code = app.get_value(omsg, ".//oub:measurement.unit.qualifier.code", True)
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        description = app.get_value(omsg, ".//oub:description", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "measurement unit qualifier description", measurement_unit_qualifier_code)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO measurement_unit_qualifier_descriptions_oplog (measurement_unit_qualifier_code, language_id, description, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (measurement_unit_qualifier_code, language_id, description, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, measurement_unit_qualifier_code)
        cur.close()
