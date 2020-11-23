import common.globals as g


class profile_44005_monetary_exchange_rate(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = g.app.get_timestamp()
        monetary_exchange_period_sid = g.app.get_number_value(omsg, ".//oub:monetary.exchange.period.sid", True)
        child_monetary_unit_code = g.app.get_value(omsg, ".//oub:child.monetary.unit.code", True)
        exchange_rate = g.app.get_value(omsg, ".//oub:exchange.rate", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "monetary exchange rate", monetary_exchange_period_sid)

        # Load data
        cur = g.app.conn.cursor()
        try:
            cur.execute("""INSERT INTO monetary_exchange_rates_oplog (monetary_exchange_period_sid,
            child_monetary_unit_code, exchange_rate,
            operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (monetary_exchange_period_sid,
            child_monetary_unit_code, exchange_rate,
            operation, operation_date))
            g.app.conn.commit()
        except:
            g.data_file.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, monetary_exchange_period_sid)
        cur.close()
