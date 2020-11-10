import common.globals as g


class profile_11000_certificate_type(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        certificate_type_code = app.get_value(omsg, ".//oub:certificate.type.code", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)

        certificate_types = g.app.get_certificate_types()

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "certificate type", certificate_type_code)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if update_type in ("1", "3"):	  # UPDATE or INSERT
                if validity_end_date is not None:
                    if validity_end_date < validity_start_date:
                        g.app.record_business_rule_violation("CET3", "The start date must be less than or equal to the end date.", operation, transaction_id, message_id, record_code, sub_record_code, certificate_type_code)

            if update_type == "1":  # UPDATE
                if certificate_type_code not in certificate_types:
                    g.app.record_business_rule_violation("DBFK", "The certificate type must exist.", operation, transaction_id, message_id, record_code, sub_record_code, certificate_type_code)
            elif update_type == "3":  # INSERT
                if certificate_type_code in certificate_types:
                    g.app.record_business_rule_violation("CET1", "The certificate type must be unique.", operation, transaction_id, message_id, record_code, sub_record_code, certificate_type_code)
            elif update_type == "2":  # DELETE
                sql = "select count(*) from certificates where certificate_type_code = %s;"
                params = [
                    certificate_type_code
                ]
                cur = g.app.conn.cursor()
                cur.execute(sql, params)
                rows = cur.fetchall()
                if len(rows) != 0:
                    g.app.record_business_rule_violation("CET2", "The certificate type cannot be deleted if it is used in a certificate.", operation, transaction_id, message_id, record_code, sub_record_code, certificate_type_code)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO certificate_types_oplog (certificate_type_code, validity_start_date,
            validity_end_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (certificate_type_code, validity_start_date,
            validity_end_date, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, certificate_type_code)
        cur.close()
