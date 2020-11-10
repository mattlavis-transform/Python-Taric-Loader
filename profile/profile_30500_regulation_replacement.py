import common.globals as g


class profile_30500_regulation_replacement(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        replacing_regulation_role = app.get_value(omsg, ".//oub:replacing.regulation.role", True)
        replacing_regulation_id = app.get_value(omsg, ".//oub:replacing.regulation.id", True)
        replaced_regulation_role = app.get_value(omsg, ".//oub:replaced.regulation.role", True)
        replaced_regulation_id = app.get_value(omsg, ".//oub:replaced.regulation.id", True)
        measure_type_id = app.get_value(omsg, ".//oub:measure.type.id", True)
        geographical_area_id = app.get_value(omsg, ".//oub:geographical.area.id", True)
        chapter_heading = app.get_value(omsg, ".//oub:chapter.heading", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "regulation replacement", replacing_regulation_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO regulation_replacements_oplog (replacing_regulation_role,
            replacing_regulation_id, replaced_regulation_role, replaced_regulation_id,
            measure_type_id, geographical_area_id, chapter_heading,
            operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (replacing_regulation_role,
            replacing_regulation_id, replaced_regulation_role, replaced_regulation_id,
            measure_type_id, geographical_area_id, chapter_heading,
            operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, replacing_regulation_id)
        cur.close()
