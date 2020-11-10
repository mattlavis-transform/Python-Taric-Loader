import common.globals as g


class profile_30000_full_temporary_stop_regulation(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        full_temporary_stop_regulation_role = app.get_value(omsg, ".//oub:full.temporary.stop.regulation.role", True)
        full_temporary_stop_regulation_id = app.get_value(omsg, ".//oub:full.temporary.stop.regulation.id", True)
        published_date = app.get_date_value(omsg, ".//oub:published.date", True)
        officialjournal_number = app.get_value(omsg, ".//oub:officialjournal.number", True)
        officialjournal_page = app.get_value(omsg, ".//oub:officialjournal.page", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)
        effective_enddate = app.get_date_value(omsg, ".//oub:effective.enddate", True)
        complete_abrogation_regulation_role = app.get_value(omsg, ".//oub:complete.abrogation.regulation.role", True)
        complete_abrogation_regulation_id = app.get_value(omsg, ".//oub:complete.abrogation.regulation.id", True)
        explicit_abrogation_regulation_role = app.get_value(omsg, ".//oub:explicit.abrogation.regulation.role", True)
        explicit_abrogation_regulation_id = app.get_value(omsg, ".//oub:explicit.abrogation.regulation.id", True)
        replacement_indicator = app.get_value(omsg, ".//oub:replacement.indicator", True)
        information_text = app.get_value(omsg, ".//oub:information.text", True)
        approved_flag = app.get_value(omsg, ".//oub:approved.flag", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "full temporary stop regulation", full_temporary_stop_regulation_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO full_temporary_stop_regulations_oplog (full_temporary_stop_regulation_role,
            full_temporary_stop_regulation_id, published_date, officialjournal_number,
            officialjournal_page,
            validity_start_date, validity_end_date, effective_enddate,
            complete_abrogation_regulation_role, complete_abrogation_regulation_id,
            explicit_abrogation_regulation_role, explicit_abrogation_regulation_id,
            replacement_indicator, information_text, approved_flag,
            operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (full_temporary_stop_regulation_role,
            full_temporary_stop_regulation_id, published_date, officialjournal_number,
            officialjournal_page,
            validity_start_date, validity_end_date, effective_enddate,
            complete_abrogation_regulation_role, complete_abrogation_regulation_id,
            explicit_abrogation_regulation_role, explicit_abrogation_regulation_id,
            replacement_indicator, information_text, approved_flag,
            operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, full_temporary_stop_regulation_id)
        cur.close()
