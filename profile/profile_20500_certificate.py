import common.globals as g


class profile_20500_certificate(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        certificate_type_code = app.get_value(omsg, ".//oub:certificate.type.code", True)
        certificate_code = app.get_value(omsg, ".//oub:certificate.code", True)
        code = certificate_type_code + certificate_code
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "certificate", certificate_type_code + certificate_code)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            certificate_types = g.app.get_certificate_types()

            # Business rule CE 3
            if validity_end_date is not None:
                if validity_end_date < validity_start_date:
                    g.app.record_business_rule_violation("CE3", "The start date must be less than or equal to the end date.", operation, transaction_id, message_id, record_code, sub_record_code, code)

            if update_type == "1":  # UPDATE
                # Business rule CE 1
                if certificate_type_code not in certificate_types:
                    g.app.record_business_rule_violation("CE1", "The referenced certificate type must exist.", operation, transaction_id, message_id, record_code, sub_record_code, code)

            elif update_type == "3":  # INSERT
                # Business rule CE 1
                if certificate_type_code not in certificate_types:
                    g.app.record_business_rule_violation("CE1", "The referenced certificate type must exist.", operation, transaction_id, message_id, record_code, sub_record_code, code)

                # Business rule CE 2
                certificates = g.app.get_certificates()
                if code in certificates:
                    g.app.record_business_rule_violation("CE2", "The combination certificate type and certificate code must be unique.", operation, transaction_id, message_id, record_code, sub_record_code, code)

            elif update_type == "2":  # DELETE
                certificates = g.app.get_used_certificates()
                if code in certificates:
                    g.app.record_business_rule_violation("CE5", "The certificate cannot be deleted if it is used in a measure condition component.", operation, transaction_id, message_id, record_code, sub_record_code, code)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO certificates_oplog (certificate_type_code, certificate_code,
            validity_start_date, validity_end_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (certificate_type_code, certificate_code, validity_start_date, validity_end_date, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, code)
        cur.close()
