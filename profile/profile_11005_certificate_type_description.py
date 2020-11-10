import common.globals as g


class profile_11005_certificate_type_description(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        certificate_type_code = app.get_value(omsg, ".//oub:certificate.type.code", True)
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        description = app.get_value(omsg, ".//oub:description", True)

        certificate_type_descriptions = g.app.get_certificate_type_descriptions()

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "certificate type description", certificate_type_code)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if update_type == "1":	  # UPDATE
                if certificate_type_code not in certificate_type_descriptions:
                    g.app.record_business_rule_violation("DBFK", "The footnote type must exist", operation, transaction_id, message_id, record_code, sub_record_code, certificate_type_code)

            elif update_type == "3":	  # INSERT
                if certificate_type_code in certificate_type_descriptions:
                    g.app.record_business_rule_violation("CET1", "The type of the certificate must be unique.", operation, transaction_id, message_id, record_code, sub_record_code, certificate_type_code)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO certificate_type_descriptions_oplog (certificate_type_code, language_id,
            description, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (certificate_type_code, language_id,
            description, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, certificate_type_code)
        cur.close()
