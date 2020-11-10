import common.globals as g


class profile_23505_measure_type_description(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        measure_type_id = app.get_value(omsg, ".//oub:measure.type.id", True)
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        description = app.get_value(omsg, ".//oub:description", True)

        measure_types = g.app.get_measure_types()

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "measure type description", measure_type_id)

        # Perform business rule validation
        if update_type == "1":  # UPDATE
            if g.app.perform_taric_validation is True:
                if measure_type_id not in measure_types:
                    g.app.record_business_rule_violation("DBFK", "The referenced measure type must exist.", operation, transaction_id, message_id, record_code, sub_record_code, measure_type_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO measure_type_descriptions_oplog (measure_type_id, language_id,
            description, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (measure_type_id, language_id,
            description, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, measure_type_id)
        cur.close()
