import common.globals as g


class profile_32505_meursing_heading_text(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        meursing_table_plan_id = app.get_number_value(omsg, ".//oub:meursing.table.plan.id", True)
        meursing_heading_number = app.get_value(omsg, ".//oub:meursing.heading.number", True)
        row_column_code = app.get_value(omsg, ".//oub:row.column.code", True)
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        description = app.get_value(omsg, ".//oub:description", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "Meursing heading text for Meursing heading number", meursing_heading_number)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO meursing_heading_texts_oplog (meursing_table_plan_id, meursing_heading_number,
            row_column_code, language_id, description, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (meursing_table_plan_id, meursing_heading_number,
            row_column_code, language_id, description, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, meursing_heading_number)
        cur.close()
