import common.globals as g


class profile_17000_publication_sigle(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        code_type_id = app.get_value(omsg, ".//oub:code.type.id", True)
        code = app.get_value(omsg, ".//oub:code", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)
        publication_code = app.get_value(omsg, ".//oub:publication.code", True)
        publication_sigle = app.get_value(omsg, ".//oub:publication.sigle", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "publication sigle", code_type_id)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if update_type in ("1", "3"):  # UPDATE or INSERT
                # Business rule PS3
                if validity_end_date is not None:
                    if validity_end_date < validity_start_date:
                        g.app.record_business_rule_violation("PS3", "The start date must be less than or equal to the end date.", operation, transaction_id, message_id, record_code, sub_record_code, publication_code)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO publication_sigles_oplog (code_type_id, code, validity_start_date,
            validity_end_date, publication_code, publication_sigle, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (code_type_id, code, validity_start_date, validity_end_date, publication_code, publication_sigle, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, publication_sigle)
        cur.close()
