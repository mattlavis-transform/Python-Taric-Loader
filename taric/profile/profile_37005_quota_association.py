from datetime import datetime
import common.globals as g


class profile_37005_quota_association(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = g.app.get_timestamp()
        main_quota_definition_sid = g.app.get_number_value(omsg, ".//oub:main.quota.definition.sid", True)
        sub_quota_definition_sid = g.app.get_number_value(omsg, ".//oub:sub.quota.definition.sid", True)
        relation_type = g.app.get_value(omsg, ".//oub:relation.type", True)
        coefficient = g.app.get_value(omsg, ".//oub:coefficient", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "quota association", main_quota_definition_sid)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            sql = "select quota_definition_sid from quota_definitions where quota_definition_sid = %s"
            params = [
                str(main_quota_definition_sid)
            ]
            cur = g.app.conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            if len(rows) == 0:
                g.data_file.record_business_rule_violation("DBFK", "Quota definition must exist.", operation, transaction_id, message_id, record_code, sub_record_code, str(main_quota_definition_sid))

            sql = "select quota_definition_sid from quota_definitions where quota_definition_sid = %s"
            params = [
                str(sub_quota_definition_sid)
            ]
            cur = g.app.conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            if len(rows) == 0:
                g.data_file.record_business_rule_violation("DBFK", "Quota definition must exist.", operation, transaction_id, message_id, record_code, sub_record_code, str(sub_quota_definition_sid))

        # Load data
        cur = g.app.conn.cursor()
        try:
            cur.execute("""INSERT INTO quota_associations (main_quota_definition_sid,
            sub_quota_definition_sid, relation_type, coefficient, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (main_quota_definition_sid,
            sub_quota_definition_sid, relation_type, coefficient, operation, operation_date))
            g.app.conn.commit()
        except:
            g.data_file.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, main_quota_definition_sid)
        cur.close()
