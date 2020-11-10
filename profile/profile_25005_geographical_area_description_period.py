import common.globals as g


class profile_25005_geographical_area_description_period(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        geographical_area_description_period_sid = app.get_number_value(omsg, ".//oub:geographical.area.description.period.sid", True)
        geographical_area_sid = app.get_number_value(omsg, ".//oub:geographical.area.sid", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        geographical_area_id = app.get_value(omsg, ".//oub:geographical.area.id", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "geographical area description period", geographical_area_description_period_sid)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO geographical_area_description_periods_oplog (geographical_area_description_period_sid,
            geographical_area_sid, validity_start_date, geographical_area_id, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (geographical_area_description_period_sid,
            geographical_area_sid, validity_start_date, geographical_area_id, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, geographical_area_id)
        cur.close()
