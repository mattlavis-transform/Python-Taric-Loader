import sys
from datetime import datetime
import common.globals as g


class profile_35000_measure_condition_code(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        condition_code = app.get_value(omsg, ".//oub:condition.code", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "measure condition", condition_code)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO measure_condition_codes_oplog (condition_code, validity_start_date,
            validity_end_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (condition_code, validity_start_date, validity_end_date, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, condition_code)
        cur.close()
