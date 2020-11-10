import common.globals as g


class profile_20505_certificate_description_period(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        certificate_description_period_sid = app.get_number_value(omsg, ".//oub:certificate.description.period.sid", True)
        certificate_type_code = app.get_value(omsg, ".//oub:certificate.type.code", True)
        certificate_code = app.get_value(omsg, ".//oub:certificate.code", True)
        code = certificate_type_code + certificate_code
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)

        certificate_types = g.app.get_certificate_types()

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "certificate", certificate_description_period_sid)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if update_type == "1":  # UPDATE
                if certificate_type_code not in certificate_types:
                    g.app.record_business_rule_violation("CE1", "The referenced certificate type must exist.", operation, transaction_id, message_id, record_code, sub_record_code, code)

            elif update_type == "3":  # INSERT
                if certificate_type_code not in certificate_types:
                    g.app.record_business_rule_violation("CE1", "The referenced certificate type must exist.", operation, transaction_id, message_id, record_code, sub_record_code, code)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO certificate_description_periods_oplog (certificate_description_period_sid,
            certificate_type_code, certificate_code, validity_start_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (certificate_description_period_sid, certificate_type_code, certificate_code, validity_start_date, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, code)
        cur.close()
