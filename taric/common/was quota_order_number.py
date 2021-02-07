from datetime import datetime
import os


class quota_order_number(object):
    def __init__(self, app, root):
        self.xml = ""
        for oTransaction in root.findall('.//oub:measure/../../../../../env:transaction', g.app.namespaces):
            update_type = self.get_value(app, oTransaction, ".//oub:record/oub:update.type")
            print(oTransaction.get("id"), update_type)
            validity_start_date = datetime.strptime(self.get_value(app, oTransaction, ".//oub:measure/oub:validity.start.date"), '%Y-%m-%d')
            validity_end_date = self.get_value(app, oTransaction, ".//oub:measure/oub:validity.start.date")
            if validity_end_date != "":
                validity_end_date = datetime.strptime(validity_end_date, '%Y-%m-%d')
                if validity_end_date > g.app.critical_date:
                    validity_end_date = g.app.critical_date.strftime('%Y-%m-%d')
                else:
                    validity_end_date = g.app.critical_date.strftime('%Y-%m-%d')
            else:
                validity_end_date = validity_end_date.strftime('%Y-%m-%d')

            if (validity_start_date < g.app.critical_date) or update_type == 2:
                self.transaction_id = oTransaction.get("id")
                self.add('<env:transaction id="' + self.transaction_id + '">', 1)
                self.add('<env:app.message id="' + str(app.message_id) + '">', 2)
                self.add('<oub:transmission xmlns:oub="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0">', 3)
                self.add("<oub:record>", 4)
                self.build(app, oTransaction, "<oub:transaction.id>", ".//oub:record/oub:transaction.id", 5)
                self.build(app, oTransaction, "<oub:record.code>", ".//oub:record/oub:record.code", 5)
                self.build(app, oTransaction, "<oub:subrecord.code>", ".//oub:record/oub:subrecord.code", 5)
                self.build(app, oTransaction, "<oub:record.sequence.number>", str(app.message_id), 5)
                self.build(app, oTransaction, "<oub:update.type>", ".//oub:record/oub:update.type", 5)
                self.add("<oub:measure>", 5)
                self.build(app, oTransaction, "<oub:measure.sid>", ".//oub:measure/oub:measure.sid", 6)
                self.build(app, oTransaction, "<oub:measure.type>", ".//oub:measure/oub:measure.type", 6)
                self.build(app, oTransaction, "<oub:geographical.area>", ".//oub:measure/oub:geographical.area", 6)
                self.build(app, oTransaction, "<oub:goods.nomenclature.item.id>", ".//oub:measure/oub:goods.nomenclature.item.id", 6)
                self.build(app, oTransaction, "<oub:additional.code.type>", ".//oub:measure/oub:additional.code.type", 6)
                self.build(app, oTransaction, "<oub:additional.code>", ".//oub:measure/oub:additional.code", 6)
                self.build(app, oTransaction, "<oub:ordernumber>", ".//oub:measure/ordernumber", 6)
                self.build(app, oTransaction, "<oub:reduction.indicator>", ".//oub:measure/oub:reduction.indicator", 6)
                self.build(app, oTransaction, "<oub:validity.start.date>", ".//oub:measure/oub:validity.start.date", 6)
                self.build(app, oTransaction, "<oub:measure.generating.regulation.role>", ".//oub:measure/oub:measure.generating.regulation.role", 6)
                self.build(app, oTransaction, "<oub:measure.generating.regulation.id>", ".//oub:measure/oub:measure.generating.regulation.id", 6)
                self.build(app, oTransaction, "<oub:validity.end.date>", str(validity_end_date), 6)
                self.build(app, oTransaction, "<oub:justification.regulation.role>", ".//oub:measure/oub:justification.regulation.role", 6)
                self.build(app, oTransaction, "<oub:justification.regulation.id>", ".//oub:measure/oub:justification.regulation.id", 6)
                self.build(app, oTransaction, "<oub:stopped.flag>", ".//oub:measure/oub:stopped.flag", 6)
                self.build(app, oTransaction, "<oub:geographical.area.sid>", ".//oub:measure/oub:geographical.area.sid", 6)
                self.build(app, oTransaction, "<oub:goods.nomenclature.sid>", ".//oub:measure/oub:goods.nomenclature.sid", 6)
                self.build(app, oTransaction, "<oub:additional.code.sid>", ".//oub:measure/oub:additional.code.sid", 6)
                self.build(app, oTransaction, "<oub:export.refund.nomenclature.sid>", ".//oub:measure/oub:export.refund.nomenclature.sid", 6)
                self.add("</oub:measure>", 5)
                self.add("</oub:record>", 4)
                self.add("</oub:transmission>", 3)
                self.add("</env:app.message>", 2)
                self.add('</env:transaction>', 1)
                g.app.message_id += 1

    def add(self, field, indent):
        self.xml += ("  " * indent) + field + "\n"

    def build(self, app, oNode, field, xpath, indent):
        if "oub" in xpath or "env" in xpath:
            s = self.get_value(app, oNode, xpath)
            if s != "":
                self.xml += ("  " * indent) + field + s + self.closeoff(field) + "\n"
        else:
            s = xpath
            self.xml += ("  " * indent) + field + s + self.closeoff(field) + "\n"

    def closeoff(self, s):
        return (s.replace("<", "</"))

    def doReplace(self, namespace, field, value):
        self.xml = self.xml.replace(field, value)

    def get_value(self, app, oNode, xpath):
        try:
            s = oNode.find(xpath, g.app.namespaces).text
        except:
            s = ""
        return (s)
