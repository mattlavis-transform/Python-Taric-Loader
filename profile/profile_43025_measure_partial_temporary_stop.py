import common.globals as g


class profile_43025_measure_partial_temporary_stop(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        measure_sid = app.get_number_value(omsg, ".//oub:measure.sid", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)
        partial_temporary_stop_regulation_id = app.get_value(omsg, ".//oub:partial.temporary.stop.regulation.id", True)
        partial_temporary_stop_regulation_officialjournal_number = app.get_value(omsg, ".//oub:partial.temporary.stop.regulation.officialjournal.number", True)
        partial_temporary_stop_regulation_officialjournal_page = app.get_value(omsg, ".//oub:partial.temporary.stop.regulation.officialjournal.page", True)
        abrogation_regulation_id = app.get_value(omsg, ".//oub:abrogation.regulation.id", True)
        abrogation_regulation_officialjournal_number = app.get_value(omsg, ".//oub:abrogation.regulation.officialjournal.number", True)
        abrogation_regulation_officialjournal_page = app.get_value(omsg, ".//oub:abrogation.regulation.officialjournal.page", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "partial temporary stop on measure_sid", measure_sid)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            sql = "select measure_sid from measures where measure_sid = %s limit 1"
            params = [
                str(measure_sid)
            ]
            cur = g.app.conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            try:
                row = rows[0]
                measure_exists = True
            except:
                measure_exists = False

            if measure_exists is False:
                g.app.record_business_rule_violation("DBFK", "Measure must exist (partial temporary stop).", operation, transaction_id, message_id, record_code, sub_record_code, str(measure_sid))

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO measure_partial_temporary_stops_oplog (measure_sid, validity_start_date, validity_end_date,
            partial_temporary_stop_regulation_id, partial_temporary_stop_regulation_officialjournal_number,
            partial_temporary_stop_regulation_officialjournal_page, abrogation_regulation_id,
            abrogation_regulation_officialjournal_number, abrogation_regulation_officialjournal_page,
            operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (measure_sid, validity_start_date, validity_end_date,
            partial_temporary_stop_regulation_id, partial_temporary_stop_regulation_officialjournal_number,
            partial_temporary_stop_regulation_officialjournal_page, abrogation_regulation_id,
            abrogation_regulation_officialjournal_number, abrogation_regulation_officialjournal_page,
            operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, measure_sid)
        cur.close()
