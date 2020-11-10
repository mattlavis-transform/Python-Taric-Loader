from datetime import datetime
import common.globals as g


class profile_44000_monetary_exchange_period(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        monetary_exchange_period_sid = app.get_number_value(omsg, ".//oub:monetary.exchange.period.sid", True)
        parent_monetary_unit_code = app.get_value(omsg, ".//oub:parent.monetary.unit.code", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "monetary exchange period", monetary_exchange_period_sid)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO monetary_exchange_periods_oplog (monetary_exchange_period_sid,
            parent_monetary_unit_code, validity_start_date, validity_end_date,
            operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (monetary_exchange_period_sid,
            parent_monetary_unit_code, validity_start_date, validity_end_date,
            operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, monetary_exchange_period_sid)
        cur.close()
