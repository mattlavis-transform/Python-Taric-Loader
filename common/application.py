import xml.etree.ElementTree as ET
import xmlschema
import psycopg2
import sys
import os
from os import system, name, path
import csv
import re
import json

from datetime import timedelta
from datetime import datetime

from common.log import log
from common.business_rule_violation import business_rule_violation
from common.regulation import regulation
from common.classification import classification

from profile.profile_10000_footnote_type import profile_10000_footnote_type
from profile.profile_10005_footnote_type_description import profile_10005_footnote_type_description
from profile.profile_11000_certificate_type import profile_11000_certificate_type
from profile.profile_11005_certificate_type_description import profile_11005_certificate_type_description
from profile.profile_12000_additional_code_type import profile_12000_additional_code_type
from profile.profile_12005_additional_code_type_description import profile_12005_additional_code_type_description
from profile.profile_13000_language import profile_13000_language
from profile.profile_13005_language_description import profile_13005_language_description
from profile.profile_14000_measure_type_series import profile_14000_measure_type_series
from profile.profile_14005_measure_type_series_description import profile_14005_measure_type_series_description
from profile.profile_15000_regulation_group import profile_15000_regulation_group
from profile.profile_15005_regulation_group_description import profile_15005_regulation_group_description
from profile.profile_16000_regulation_role_type import profile_16000_regulation_role_type
from profile.profile_16005_regulation_role_type_description import profile_16005_regulation_role_type_description
from profile.profile_17000_publication_sigle import profile_17000_publication_sigle
from profile.profile_20000_footnote import profile_20000_footnote
from profile.profile_20005_footnote_description_period import profile_20005_footnote_description_period
from profile.profile_20010_footnote_description import profile_20010_footnote_description
from profile.profile_20500_certificate import profile_20500_certificate
from profile.profile_20505_certificate_description_period import profile_20505_certificate_description_period
from profile.profile_20510_certificate_description import profile_20510_certificate_description
from profile.profile_21000_measurement_unit import profile_21000_measurement_unit
from profile.profile_21005_measurement_unit_description import profile_21005_measurement_unit_description
from profile.profile_21500_measurement_unit_qualifier import profile_21500_measurement_unit_qualifier
from profile.profile_21505_measurement_unit_qualifier_description import profile_21505_measurement_unit_qualifier_description
from profile.profile_22000_measurement import profile_22000_measurement
from profile.profile_22500_monetary_unit import profile_22500_monetary_unit
from profile.profile_22505_monetary_unit_description import profile_22505_monetary_unit_description
from profile.profile_23000_duty_expression import profile_23000_duty_expression
from profile.profile_23005_duty_expression_description import profile_23005_duty_expression_description
from profile.profile_23500_measure_type import profile_23500_measure_type
from profile.profile_23505_measure_type_description import profile_23505_measure_type_description
from profile.profile_24000_additional_code_type_measure_type import profile_24000_additional_code_type_measure_type
from profile.profile_24500_additional_code import profile_24500_additional_code
from profile.profile_24505_additional_code_description_period import profile_24505_additional_code_description_period
from profile.profile_24510_additional_code_description import profile_24510_additional_code_description
from profile.profile_24515_footnote_association_additional_code import profile_24515_footnote_association_additional_code
from profile.profile_25000_geographical_area import profile_25000_geographical_area
from profile.profile_25005_geographical_area_description_period import profile_25005_geographical_area_description_period
from profile.profile_25010_geographical_area_description import profile_25010_geographical_area_description
from profile.profile_25015_geographical_area_membership import profile_25015_geographical_area_membership
from profile.profile_27000_goods_nomenclature_group import profile_27000_goods_nomenclature_group
from profile.profile_27005_goods_nomenclature_group_description import profile_27005_goods_nomenclature_group_description
from profile.profile_27500_complete_abrogation_regulation import profile_27500_complete_abrogation_regulation
from profile.profile_28000_explicit_abrogation_regulation import profile_28000_explicit_abrogation_regulation
from profile.profile_28500_base_regulation import profile_28500_base_regulation
from profile.profile_29000_modification_regulation import profile_29000_modification_regulation
from profile.profile_29500_prorogation_regulation import profile_29500_prorogation_regulation
from profile.profile_29505_prorogation_regulation_action import profile_29505_prorogation_regulation_action
from profile.profile_30000_full_temporary_stop_regulation import profile_30000_full_temporary_stop_regulation
from profile.profile_30005_fts_regulation_action import profile_30005_fts_regulation_action
from profile.profile_30500_regulation_replacement import profile_30500_regulation_replacement
from profile.profile_32000_meursing_table_plan import profile_32000_meursing_table_plan
from profile.profile_32500_meursing_heading import profile_32500_meursing_heading
from profile.profile_32505_meursing_heading_text import profile_32505_meursing_heading_text
from profile.profile_32510_footnote_association_meursing_heading import profile_32510_footnote_association_meursing_heading
from profile.profile_33000_meursing_subheading import profile_33000_meursing_subheading
from profile.profile_34000_meursing_additional_code import profile_34000_meursing_additional_code
from profile.profile_34005_meursing_table_cell_component import profile_34005_meursing_table_cell_component
from profile.profile_35000_measure_condition_code import profile_35000_measure_condition_code
from profile.profile_35005_measure_condition_code_description import profile_35005_measure_condition_code_description
from profile.profile_35500_measure_action import profile_35500_measure_action
from profile.profile_35505_measure_action_description import profile_35505_measure_action_description
from profile.profile_36000_quota_order_number import profile_36000_quota_order_number
from profile.profile_36010_quota_order_number_origin import profile_36010_quota_order_number_origin
from profile.profile_36015_quota_order_number_origin_exclusion import profile_36015_quota_order_number_origin_exclusion
from profile.profile_37000_quota_definition import profile_37000_quota_definition
from profile.profile_37005_quota_association import profile_37005_quota_association
from profile.profile_37010_quota_blocking_period import profile_37010_quota_blocking_period
from profile.profile_37015_quota_suspension_period import profile_37015_quota_suspension_period
from profile.profile_37500_quota_balance_event import profile_37500_quota_balance_event
from profile.profile_37505_quota_unblocking_event import profile_37505_quota_unblocking_event
from profile.profile_37510_quota_critical_event import profile_37510_quota_critical_event
from profile.profile_37515_quota_exhaustion_event import profile_37515_quota_exhaustion_event
from profile.profile_37520_quota_reopening_event import profile_37520_quota_reopening_event
from profile.profile_37525_quota_unsuspension_event import profile_37525_quota_unsuspension_event
from profile.profile_37530_quota_closed_and_balance_transferred_event import profile_37530_quota_closed_and_balance_transferred_event
from profile.profile_40000_goods_nomenclature import profile_40000_goods_nomenclature
from profile.profile_40005_goods_nomenclature_indent import profile_40005_goods_nomenclature_indent
from profile.profile_40010_goods_nomenclature_description_period import profile_40010_goods_nomenclature_description_period
from profile.profile_40015_goods_nomenclature_description import profile_40015_goods_nomenclature_description
from profile.profile_40020_footnote_association_goods_nomenclature import profile_40020_footnote_association_goods_nomenclature
from profile.profile_40025_nomenclature_group_membership import profile_40025_nomenclature_group_membership
from profile.profile_40035_goods_nomenclature_origin import profile_40035_goods_nomenclature_origin
from profile.profile_40040_goods_nomenclature_successor import profile_40040_goods_nomenclature_successor
from profile.profile_41000_export_refund_nomenclature import profile_41000_export_refund_nomenclature
from profile.profile_41005_export_refund_nomenclature_indent import profile_41005_export_refund_nomenclature_indent
from profile.profile_41010_export_refund_nomenclature_description_period import profile_41010_export_refund_nomenclature_description_period
from profile.profile_41015_export_refund_nomenclature_description import profile_41015_export_refund_nomenclature_description
from profile.profile_41020_footnote_association_ern import profile_41020_footnote_association_ern
from profile.profile_43000_measure import profile_43000_measure
from profile.profile_43005_measure_component import profile_43005_measure_component
from profile.profile_43010_measure_condition import profile_43010_measure_condition
from profile.profile_43011_measure_condition_component import profile_43011_measure_condition_component
from profile.profile_43015_measure_excluded_geographical_area import profile_43015_measure_excluded_geographical_area
from profile.profile_43020_footnote_association_measure import profile_43020_footnote_association_measure
from profile.profile_43025_measure_partial_temporary_stop import profile_43025_measure_partial_temporary_stop
from profile.profile_44000_monetary_exchange_period import profile_44000_monetary_exchange_period
from profile.profile_44005_monetary_exchange_rate import profile_44005_monetary_exchange_rate


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class application(object):
    def __init__(self):
        self.clear()
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.BASE_DIR = os.path.join(self.BASE_DIR, "..")
        self.CSV_DIR = os.path.join(self.BASE_DIR, "csv")
        self.IMPORT_DIR = os.path.join(self.BASE_DIR, "import")
        self.TEMP_DIR = os.path.join(self.BASE_DIR, "temp")
        self.TEMP_FILE = os.path.join(self.TEMP_DIR, "temp.xml")
        self.LOG_DIR = os.path.join(self.BASE_DIR, "log")
        self.IMPORT_LOG_DIR = os.path.join(self.LOG_DIR, "import")
        self.ERROR_LOG_DIR = os.path.join(self.LOG_DIR, "errors")
        self.LOG_FILE = os.path.join(self.LOG_DIR, "log.csv")
        self.CONFIG_DIR = os.path.join(self.BASE_DIR, "config")
        self.CONFIG_FILE = os.path.join(self.CONFIG_DIR, "config.json")
        self.CONFIG_FILE_LOCAL = os.path.join(
            self.CONFIG_DIR, "config_convert_and_import_taric_files.json")

        self.SCHEMA_DIR = os.path.join(self.BASE_DIR, "..")
        self.SCHEMA_DIR = os.path.join(self.SCHEMA_DIR, "xsd")

        self.namespaces = {'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0',
                           'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0', }  # add more as needed
        self.message_id = 1
        self.debug = True
        self.simple_filenames = True

        self.correlation_id = ""
        self.checksum = ""
        self.filesize = ""
        self.source_file_name = ""

        self.log_list_string = []
        self.log_list = []

        self.load_errors = []
        self.business_rule_violations = []

    def num_to_bool(self, num):
        if num == 0:
            return False
        else:
            return True

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                "dbname=" + self.DBASE + " user=postgres password=" + self.p)
            self.print_to_terminal(
                "Connected to database '{0}'".format(self.DBASE), False)
        except:
            self.print_to_terminal(
                "Could not connect to database '{0}'".format(self.DBASE), False)
            sys.exit()

    def get_config(self):
        # Get global config items
        with open(self.CONFIG_FILE, 'r') as f:
            my_dict = json.load(f)

        critical_date = my_dict['critical_date']
        self.critical_date = datetime.strptime(critical_date, '%Y-%m-%d')
        self.critical_date_plus_one = self.critical_date + timedelta(days=1)
        self.critical_date_plus_one_string = datetime.strftime(
            self.critical_date_plus_one, '%Y-%m-%d')

        self.p = my_dict['p']

        self.perform_taric_validation = self.num_to_bool(
            my_dict['perform_taric_validation'])
        self.show_progress = self.num_to_bool(my_dict['show_progress'])

    def get_deleted_goods_nomenclatures(self):
        self.deleted_goods_nomenclatures = []
        sql = """select distinct goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix
        from ml.deleted_goods_nomenclatures
        order by goods_nomenclature_item_id, productline_suffix"""
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            obj = [row[0], row[1], row[2]]
            self.deleted_goods_nomenclatures.append(obj)

    def promote_fix_file(self, files):
        self.files_prepend = []
        self.files_append = []
        # If there is anything with a plus sign at the start, then this should be loaded before the primary file.
        # It must have the same envelope ID as the primary file
        for file in files:
            if file[0] == "+":
                self.files_prepend.append(file)

        # Then add in the ohrer files
        for file in files:
            if file[0] != "+":
                self.files_append.append(file)

    def validate(self, temp=False):
        # Needs work to simplify
        if temp:
            s = self.TEMP_FILE
            msg = "Validating the initial XML file against the Taric 3 schema"
        else:
            s = self.xml_file_out
            msg = "Validating the final XML file against the Taric 3 schema"

        self.print_to_terminal(msg)
        schema_path = os.path.join(self.SCHEMA_DIR, "envelope.xsd")
        my_schema = xmlschema.XMLSchema(schema_path)
        try:
            if my_schema.is_valid(s):
                self.print_to_terminal("The file validated successfully")
                success = True
            else:
                self.print_to_terminal("The file did not validate")
                success = False
        except:
            self.print_to_terminal(
                "The file did not validate and crashed the validator")
            success = False

        if success is False:
            my_schema.validate(s)

    def get_value(self, node, xpath, return_null=False):
        try:
            s = node.find(xpath, self.namespaces).text
        except:
            if return_null:
                s = None
            else:
                s = ""
        return (s)

    def get_number_value(self, node, xpath, return_null=False):
        try:
            s = int(node.find(xpath, self.namespaces).text)
        except:
            if return_null:
                s = None
            else:
                s = ""
        return (s)

    def get_node(self, node, xpath):
        try:
            s = node.find(xpath, self.namespaces)
        except:
            s = None
        return (s)

    def get_date_value(self, node, xpath, return_null=False):
        try:
            s = node.find(xpath, self.namespaces).text
            pos = s.find("T")
            if pos > -1:
                s = s[0:pos]
            s = datetime.strptime(s, "%Y-%m-%d")
        except:
            if return_null:
                s = None
            else:
                s = ""
        return (s)

    def get_index(self, node, xpath):
        index = -1
        for child in node.iter():
            index += 1
            s = child.tag.replace(
                "{urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0}", "")
            if s == xpath:
                break
        return index

    def print_to_terminal(self, s, include_indent=True):
        if self.debug:
            if include_indent:
                s = "- " + s
            else:
                s = "\n" + s.upper()
            print(s)

    def get_timestamp(self):
        ts = datetime.now()
        ts_string = datetime.strftime(ts, "%Y%m%dT%H%M%S")
        return (ts_string)

    def get_datestamp(self):
        ts = datetime.now()
        ts_string = datetime.strftime(ts, "%Y-%m-%d")
        return (ts_string)

    def clear(self):
        # for windows
        if name == 'nt':
            _ = system('cls')
        else:
            _ = system("printf '\33c\e[3J'")

    def doprint(self, s):
        self.log_handle.write(
            "Message " + str(self.message_count) + " - " + s + "\n")
        if self.show_progress is True:
            print(s)

    def load_classification_trees(self):
        if self.perform_taric_validation is True:
            self.nodes = []
            print("Getting classification trees")
            temp = []
            self.nodes.append(temp)
            for i in range(1, 100):
                chapter = str(i).zfill(2)
                filename = os.path.join(self.CSV_DIR, chapter + ".csv")
                try:
                    with open(filename) as csv_file:
                        csv_reader = csv.reader(csv_file, delimiter=",")
                        temp = []
                        for row in csv_reader:
                            c = classification(row[0], row[1], int(
                                row[2]), int(row[3]), int(row[4]))
                            temp.append(c)
                except:
                    temp = []
                self.nodes.append(temp)
                a = 1

            print("Classification trees complete")
            print("Working out nomenclature parent / child relationships")

            for i in range(1, 100):
                nodes = self.nodes[i]
                goods_nomenclature_count = len(nodes)
                for loop1 in range(0, goods_nomenclature_count):
                    my_commodity = nodes[loop1]
                    if my_commodity.significant_digits == 2:
                        pass
                    else:
                        if my_commodity.number_indents == 0:
                            for loop2 in range(loop1 - 1, -1, -1):
                                prior_commodity = nodes[loop2]
                                if prior_commodity.significant_digits == 2:
                                    my_commodity.parent_goods_nomenclature_item_id = prior_commodity.goods_nomenclature_item_id
                                    my_commodity.parent_productline_suffix = prior_commodity.productline_suffix
                                    break
                        else:
                            for loop2 in range(loop1 - 1, -1, -1):
                                prior_commodity = nodes[loop2]
                                if prior_commodity.number_indents == (my_commodity.number_indents - 1):
                                    my_commodity.parent_goods_nomenclature_item_id = prior_commodity.goods_nomenclature_item_id
                                    my_commodity.parent_productline_suffix = prior_commodity.productline_suffix
                                    break
            print("Working out nomenclature parent / child relationships - complete")

    def find_node(self, commodity_code):
        node = None
        chapter = commodity_code[0:2]
        int_chapter = int(chapter)
        nodes = self.nodes[int_chapter]
        my_index = -1
        for node in nodes:
            my_index += 1
            if node.goods_nomenclature_item_id == commodity_code and node.productline_suffix == "80":
                # Add myself to the list of relations first - will check ME01
                relations = []
                relations = self.get_relations(
                    my_index, node, "both", relations)
                node.relations = relations
                break
        return (node)

    def get_relations(self, my_index, my_commodity, direction, relations):
        chapter = my_commodity.goods_nomenclature_item_id[0:2]
        int_chapter = int(chapter)
        nodes = self.nodes[int_chapter]
        # Search UP the tree for parentage
        if direction != "down":
            for loop2 in range(my_index - 1, -1, -1):
                prior_commodity = nodes[loop2]
                if prior_commodity.goods_nomenclature_item_id == my_commodity.parent_goods_nomenclature_item_id and prior_commodity.productline_suffix == my_commodity.parent_productline_suffix:
                    if prior_commodity.productline_suffix == "80":
                        relations.append(
                            prior_commodity.goods_nomenclature_item_id)
                        a = 1
                    self.get_relations(loop2, prior_commodity, "up", relations)
                    if prior_commodity.significant_digits == 4:
                        break

        # Now search DOWN the tree for children
        if direction != "up":
            for loop2 in range(my_index + 1, len(nodes)):
                next_commodity = nodes[loop2]
                if next_commodity.parent_goods_nomenclature_item_id == my_commodity.goods_nomenclature_item_id and next_commodity.parent_productline_suffix == my_commodity.productline_suffix:
                    if next_commodity.productline_suffix == "80":
                        relations.append(
                            next_commodity.goods_nomenclature_item_id)
                    self.get_relations(
                        loop2, next_commodity, "down", relations)

                    if next_commodity.significant_digits == 4 or loop2 == len(nodes):
                        break
        return (relations)

    def get_measure_types_that_require_components(self):
        self.measure_types_that_require_components_list = []
        sql = """
        select measure_type_id from measure_types
        where measure_component_applicable_code = 1
        and validity_end_date is null
        order by 1
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) > 0:
            for rw in rows:
                self.measure_types_that_require_components_list.append(rw[0])

    def import_xml(self, xml_file, prompt=True):
        self.get_measure_types_that_require_components()
        self.duty_measure_list = []
        ret = sys.gettrace()
        if ret is None:
            self.debug_mode = False
        else:
            self.debug_mode = True

        self.import_file = xml_file
        self.load_classification_trees()
        self.all_regulations = self.get_all_regulations()
        self.geographical_area_sids = self.get_all_geographical_area_sids()
        self.geographical_areas = self.get_all_geographical_areas()
        self.quota_order_numbers = self.get_quota_order_numbers()
        self.quota_definitions = self.get_quota_definitions()
        self.goods_nomenclatures = self.get_all_goods_nomenclatures()
        self.duty_expressions = self.get_duty_expressions()

        self.print_to_terminal(
            "Preparing to import file " + xml_file + " into database " + self.DBASE, False)

        if prompt:
            ret = self.yes_or_no("Do you want to continue?")
            if not (ret) or ret in ("n", "N", "No"):
                sys.exit()

        # Check that this file has not already been imported
        sql = "SELECT import_file FROM ml.import_files WHERE import_file = %s"
        params = [
            xml_file
        ]
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

        if self.debug_mode is False:
            if len(rows) > 0:
                print("\nFile", xml_file,
                      "has already been imported - Aborting now\n")
                return

        self.xml_file_In = os.path.join(self.IMPORT_DIR, xml_file)
        self.IMPORT_LOG_FILE = os.path.join(
            self.IMPORT_LOG_DIR, "log_" + xml_file)
        self.IMPORT_LOG_FILE = self.IMPORT_LOG_FILE.replace("xml", "txt")

        self.log_handle = open(self.IMPORT_LOG_FILE, "w")

        # Load file
        ET.register_namespace(
            'oub', 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0')
        ET.register_namespace(
            'env', 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0')
        try:
            tree = ET.parse(self.xml_file_In)
        except:
            print(
                "The selected file could not be found or is not a valid, well-formed XML file")
            sys.exit(0)
        root = tree.getroot()

        self.register_import_start(xml_file)

        action_list = ["update", "delete", "insert"]

        self.message_count = 0

        for oTransaction in root.findall('.//env:transaction', self.namespaces):
            for omsg in oTransaction.findall('.//env:app.message', self.namespaces):
                record_code = omsg.find(
                    ".//oub:record.code", self.namespaces).text
                sub_record_code = omsg.find(
                    ".//oub:subrecord.code", self.namespaces).text
                update_type = omsg.find(
                    ".//oub:update.type", self.namespaces).text
                transaction_id = omsg.find(
                    ".//oub:transaction.id", self.namespaces).text
                message_id = omsg.attrib["id"]  # message id dummy"

                # 10000	FOOTNOTE TYPE
                if record_code == "100" and sub_record_code == "00":
                    o = profile_10000_footnote_type()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 10000	FOOTNOTE TYPE DESCRIPTION
                if record_code == "100" and sub_record_code == "05":
                    o = profile_10005_footnote_type_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 11000	CERTIFICATE TYPE
                if record_code == "110" and sub_record_code == "00":
                    o = profile_11000_certificate_type()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 11005	CERTIFICATE TYPE DESCRIPTION
                if record_code == "110" and sub_record_code == "05":
                    o = profile_11005_certificate_type_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 12000	ADDITIONAL CODE TYPE
                if record_code == "120" and sub_record_code == "00":
                    o = profile_12000_additional_code_type()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 12005	ADDITIONAL CODE TYPE DESCRIPTION
                if record_code == "120" and sub_record_code == "05":
                    o = profile_12005_additional_code_type_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 13000	LANGUAGE
                if record_code == "130" and sub_record_code == "00":
                    o = profile_13000_language()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 13005	LANGUAGE DESCRIPTION
                if record_code == "130" and sub_record_code == "05":
                    o = profile_13005_language_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 14000	MEASURE TYPE SERIES
                if record_code == "140" and sub_record_code == "00":
                    o = profile_14000_measure_type_series()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 14005	MEASURE TYPE SERIES DESCRIPTION
                if record_code == "140" and sub_record_code == "05":
                    o = profile_14005_measure_type_series_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 15000	REGULATION GROUP
                if record_code == "150" and sub_record_code == "00":
                    o = profile_15000_regulation_group()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 15005	REGULATION GROUP DESCRIPTION
                if record_code == "150" and sub_record_code == "05":
                    o = profile_15005_regulation_group_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 16000	REGULATION ROLE TYPE
                if record_code == "160" and sub_record_code == "00":
                    o = profile_16000_regulation_role_type()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 16005	REGULATION ROLE TYPE DESCRIPTION
                if record_code == "160" and sub_record_code == "05":
                    o = profile_16005_regulation_role_type_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 17000	PUBLICATION SIGLE
                if record_code == "170" and sub_record_code == "00":
                    o = profile_17000_publication_sigle()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 20000	FOOTNOTE
                if record_code == "200" and sub_record_code == "00":
                    o = profile_20000_footnote()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 20005	FOOTNOTE DESCRIPTION PERIOD
                if record_code == "200" and sub_record_code == "05":
                    o = profile_20005_footnote_description_period()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 20010	FOOTNOTE DESCRIPTION
                if record_code == "200" and sub_record_code == "10":
                    o = profile_20010_footnote_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 20500	CERTIFICATE
                if record_code == "205" and sub_record_code == "00":
                    o = profile_20500_certificate()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 20505	CERTIFICATE DESCRIPTION PERIOD
                if record_code == "205" and sub_record_code == "05":
                    o = profile_20505_certificate_description_period()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 20510	CERTIFICATE DESCRIPTION
                if record_code == "205" and sub_record_code == "10":
                    o = profile_20510_certificate_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 21000	MEASUREMENT UNIT
                if record_code == "210" and sub_record_code == "00":
                    o = profile_21000_measurement_unit()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 21005	MEASUREMENT UNIT DESCRIPTION
                if record_code == "210" and sub_record_code == "05":
                    o = profile_21005_measurement_unit_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 21500	MEASUREMENT UNIT QUALIFIER
                if record_code == "215" and sub_record_code == "00":
                    o = profile_21500_measurement_unit_qualifier()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 21505	MEASUREMENT UNIT QUALIFIER DESCRIPTION
                if record_code == "215" and sub_record_code == "05":
                    o = profile_21505_measurement_unit_qualifier_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 22000	MEASUREMENT
                if record_code == "220" and sub_record_code == "00":
                    o = profile_22000_measurement()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 22500	MONETARY UNIT
                if record_code == "225" and sub_record_code == "00":
                    o = profile_22500_monetary_unit()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 22500	MONETARY UNIT
                if record_code == "225" and sub_record_code == "05":
                    o = profile_22505_monetary_unit_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 23000	DUTY EXPRESSION
                if record_code == "230" and sub_record_code == "00":
                    o = profile_23000_duty_expression()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 23005	DUTY EXPRESSION DESCRIPTION
                if record_code == "230" and sub_record_code == "05":
                    o = profile_23005_duty_expression_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 23500	MEASURE TYPE
                if record_code == "235" and sub_record_code == "00":
                    o = profile_23500_measure_type()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 23505	MEASURE TYPE DESCRIPTION
                if record_code == "235" and sub_record_code == "05":
                    o = profile_23505_measure_type_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 24000	ADDITIONAL CODE TYPE / MEASURE TYPE
                if record_code == "240" and sub_record_code == "00":
                    o = profile_24000_additional_code_type_measure_type()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 24500	ADDITIONAL CODE
                if record_code == "245" and sub_record_code == "00":
                    o = profile_24500_additional_code()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 24505	ADDITIONAL CODE DESCRIPTION PERIOD
                if record_code == "245" and sub_record_code == "05":
                    o = profile_24505_additional_code_description_period()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 24510	ADDITIONAL CODE DESCRIPTION
                if record_code == "245" and sub_record_code == "10":
                    o = profile_24510_additional_code_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 24515	FOOTNOTE ASSOCIATION - ADDITIONAL CODE
                if record_code == "245" and sub_record_code == "15":
                    o = profile_24515_footnote_association_additional_code()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 25000	GEOGRAPHICAL AREA
                if record_code == "250" and sub_record_code == "00":
                    o = profile_25000_geographical_area()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 25005	GEOGRAPHICAL AREA DESCRIPTION PERIOD
                if record_code == "250" and sub_record_code == "05":
                    o = profile_25005_geographical_area_description_period()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 25010	GEOGRAPHICAL AREA DESCRIPTION
                if record_code == "250" and sub_record_code == "10":
                    o = profile_25010_geographical_area_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 25015	GEOGRAPHICAL AREA MEMBERSHIP
                if record_code == "250" and sub_record_code == "15":
                    o = profile_25015_geographical_area_membership()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 27000	GOODS NOMENCLATURE GROUP
                if record_code == "270" and sub_record_code == "00":
                    o = profile_27000_goods_nomenclature_group()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 27000	GOODS NOMENCLATURE GROUP DESCRIPTION
                if record_code == "270" and sub_record_code == "05":
                    o = profile_27005_goods_nomenclature_group_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 27500	COMPLETE ABROGATION REGULATION
                if record_code == "275" and sub_record_code == "00":
                    o = profile_27500_complete_abrogation_regulation()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 28000	EXPLICIT ABROGATION REGULATION
                if record_code == "280" and sub_record_code == "00":
                    o = profile_28000_explicit_abrogation_regulation()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 28500	BASE REGULATION
                if record_code == "285" and sub_record_code == "00":
                    o = profile_28500_base_regulation()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 29000	MODIFICATION REGULATION
                if record_code == "290" and sub_record_code == "00":
                    o = profile_29000_modification_regulation()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 29500	PROROGATION REGULATION
                if record_code == "295" and sub_record_code == "00":
                    o = profile_29500_prorogation_regulation()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 29505	PROROGATION REGULATION ACTION
                if record_code == "295" and sub_record_code == "05":
                    o = profile_29505_prorogation_regulation_action()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 30000	FULL TEMPORARY STOP REGULATION
                if record_code == "300" and sub_record_code == "00":
                    o = profile_30000_full_temporary_stop_regulation()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 30005	FTS REGULATION ACTION
                if record_code == "300" and sub_record_code == "05":
                    o = profile_30005_fts_regulation_action()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 30500	REGULATION REPLACEMENT
                if record_code == "305" and sub_record_code == "00":
                    o = profile_30500_regulation_replacement()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 32000	MEURSING TABLE PLAN
                if record_code == "320" and sub_record_code == "00":
                    o = profile_32000_meursing_table_plan()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 32500	MEURSING HEADING
                if record_code == "325" and sub_record_code == "00":
                    o = profile_32500_meursing_heading()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 32500	MEURSING HEADING TEXT
                if record_code == "325" and sub_record_code == "05":
                    o = profile_32505_meursing_heading_text()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 32510	MEURSING HEADING TEXT
                if record_code == "325" and sub_record_code == "10":
                    o = profile_32510_footnote_association_meursing_heading()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 33000	MEURSING SUBHEADING
                if record_code == "330" and sub_record_code == "00":
                    o = profile_33000_meursing_subheading()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 34000	MEURSING SUBHEADING
                if record_code == "340" and sub_record_code == "00":
                    o = profile_34000_meursing_additional_code()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 34005	MEURSING TABLE CELL COMPONENT
                if record_code == "340" and sub_record_code == "05":
                    o = profile_34005_meursing_table_cell_component()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 35000	MEASURE CONDITION
                if record_code == "350" and sub_record_code == "00":
                    o = profile_35000_measure_condition_code()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 35005	MEASURE CONDITION DESCRIPTION
                if record_code == "350" and sub_record_code == "05":
                    o = profile_35005_measure_condition_code_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 35500	MEASURE ACTION
                if record_code == "355" and sub_record_code == "00":
                    o = profile_35500_measure_action()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 35505	MEASURE ACTION DESCRIPTION
                if record_code == "355" and sub_record_code == "05":
                    o = profile_35505_measure_action_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 36000	QUOTA ORDER NUMBER
                if record_code == "360" and sub_record_code == "00":
                    o = profile_36000_quota_order_number()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 36005	QUOTA ORDER NUMBER ORIGIN
                if record_code == "360" and sub_record_code == "10":
                    o = profile_36010_quota_order_number_origin()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 36000	QUOTA ORDER NUMBER ORIGIN EXCLUSION
                if record_code == "360" and sub_record_code == "15":
                    o = profile_36015_quota_order_number_origin_exclusion()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 37000	QUOTA DEFINITION
                if record_code == "370" and sub_record_code == "00":
                    o = profile_37000_quota_definition()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 37005	QUOTA ASSOCIATION
                if record_code == "370" and sub_record_code == "05":
                    o = profile_37005_quota_association()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 37010	QUOTA BLOCKING PERIOD
                if record_code == "370" and sub_record_code == "10":
                    o = profile_37010_quota_blocking_period()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 37015	QUOTA SUSPENSION PERIOD
                if record_code == "370" and sub_record_code == "15":
                    o = profile_37015_quota_suspension_period()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 37500	QUOTA BALANCE EVENT
                if record_code == "375" and sub_record_code == "00":
                    o = profile_37500_quota_balance_event()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 37505	QUOTA UNBLOCKING EVENT
                if record_code == "375" and sub_record_code == "05":
                    o = profile_37505_quota_unblocking_event()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 37510	QUOTA CRITICAL EVENT
                if record_code == "375" and sub_record_code == "10":
                    o = profile_37510_quota_critical_event()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 37515	QUOTA EXHAUSTION EVENT
                if record_code == "375" and sub_record_code == "15":
                    o = profile_37515_quota_exhaustion_event()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 37520	QUOTA REOPENING EVENT
                if record_code == "375" and sub_record_code == "20":
                    o = profile_37520_quota_reopening_event()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 37525	QUOTA UNSUSPENSION EVENT
                if record_code == "375" and sub_record_code == "25":
                    o = profile_37525_quota_unsuspension_event()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 37530	CLOSED AND BALANCE TRANSFER EVENT
                if record_code == "375" and sub_record_code == "30":
                    o = profile_37530_quota_closed_and_balance_transferred_event()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 40000	GOODS NOMENCLATURE
                if record_code == "400" and sub_record_code == "00":
                    o = profile_40000_goods_nomenclature()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 40005	GOODS NOMENCLATURE INDENT
                if record_code == "400" and sub_record_code == "05":
                    o = profile_40005_goods_nomenclature_indent()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 40010	GOODS NOMENCLATURE DESCRIPTION PERIOD
                if record_code == "400" and sub_record_code == "10":
                    o = profile_40010_goods_nomenclature_description_period()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 40015	GOODS NOMENCLATURE DESCRIPTION
                if record_code == "400" and sub_record_code == "15":
                    o = profile_40015_goods_nomenclature_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 40020	FOOTNOTE ASSOCIATION GOODS NOMENCLATURE
                if record_code == "400" and sub_record_code == "20":
                    o = profile_40020_footnote_association_goods_nomenclature()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 40025 NOMENCLATURE GROUP MEMBERSHIP
                if record_code == "400" and sub_record_code == "25":
                    o = profile_40025_nomenclature_group_membership()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 40035	GOODS NOMENCLATURE ORIGIN
                if record_code == "400" and sub_record_code == "35":
                    o = profile_40035_goods_nomenclature_origin()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 40040	GOODS NOMENCLATURE SUCCESSOR
                if record_code == "400" and sub_record_code == "40":
                    o = profile_40040_goods_nomenclature_successor()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 41000	EXPORT REFUND NOMENCLATURE
                if record_code == "410" and sub_record_code == "00":
                    o = profile_41000_export_refund_nomenclature()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 41000	EXPORT REFUND NOMENCLATURE INDENT
                if record_code == "410" and sub_record_code == "05":
                    o = profile_41005_export_refund_nomenclature_indent()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 41000	EXPORT REFUND NOMENCLATURE DESCRIPTION PERIOD
                if record_code == "410" and sub_record_code == "10":
                    o = profile_41010_export_refund_nomenclature_description_period()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 41000	EXPORT REFUND NOMENCLATURE DESCRIPTION
                if record_code == "410" and sub_record_code == "15":
                    o = profile_41015_export_refund_nomenclature_description()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 41000	FOOTNOTE ASSOCIATION - ERN
                if record_code == "410" and sub_record_code == "20":
                    o = profile_41020_footnote_association_ern()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 43000	MEASURE
                if record_code == "430" and sub_record_code == "00":
                    o = profile_43000_measure()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 43005	MEASURE COMPONENT
                if record_code == "430" and sub_record_code == "05":
                    o = profile_43005_measure_component()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 43010	MEASURE CONDITION
                if record_code == "430" and sub_record_code == "10":
                    o = profile_43010_measure_condition()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 43011	MEASURE CONDITION COMPONENT
                if record_code == "430" and sub_record_code == "11":
                    o = profile_43011_measure_condition_component()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 43015	MEASURE EXCLUDED GEOGRAPHICAL AREA
                if record_code == "430" and sub_record_code == "15":
                    o = profile_43015_measure_excluded_geographical_area()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 43020	FOOTNOTE ASSOCIATION - MEASURE
                if record_code == "430" and sub_record_code == "20":
                    o = profile_43020_footnote_association_measure()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 43025	MEASURE PARTIAL TEMPORARY STOP
                if record_code == "430" and sub_record_code == "25":
                    o = profile_43025_measure_partial_temporary_stop()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 44000	MONETARY EXCHANGE PERIOD
                if record_code == "440" and sub_record_code == "00":
                    o = profile_44000_monetary_exchange_period()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

                # 44005	MONETARY EXCHANGE RATE
                if record_code == "440" and sub_record_code == "05":
                    o = profile_44005_monetary_exchange_rate()
                    o.import_node(self, update_type, omsg, transaction_id,
                                  message_id, record_code, sub_record_code)

        self.log_handle.close()

        if self.perform_taric_validation is True:
            # Post load checks
            self.rule_FO04()
            self.rule_ACN5()
            self.rule_CE06()
            self.rule_GA3()
            self.rule_ME40()
            self.rule_ME43()

        # Register the load
        self.register_import_complete(xml_file)
        print(bcolors.ENDC)
        if len(self.business_rule_violations) > 0:
            print("File failed to load - rolling back")
            self.rollback()
            self.create_error_report()
        else:
            self.print_to_terminal("Load complete with no errors", False)

    def rule_ME40(self):
        return
        print("Checking rule ME40")
        # Check to see that all measures have at least one component associated with them
        # if they are measures that require components
        my_string = ""
        for item in self.duty_measure_list:
            my_string += str(item) + ", "

        my_string = my_string.strip()
        my_string = my_string.strip(",")

        if my_string != "":
            sql = """select m.measure_sid, count(mc.*) as component_count
            from measures m left outer join measure_components mc
            on m.measure_sid = mc.measure_sid where m.measure_sid in (%s)
            group by m.measure_sid order by m.measure_sid"""
            params = [
                my_string
            ]
            cur = self.conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            my_list = []
            for row in rows:
                if int(row[1]) == 0:
                    my_list.append(row[0])

            if len(my_list) > 0:
                for measure_sid in my_list:
                    self.record_business_rule_violation("ME40", "'If the flag 'duty expression' on measure type is 'mandatory' then at least "
                                                        "one measure component or measure condition component record must be specified. If the flag is set 'not permitted' then "
                                                        "no measure component or measure condition component must exist. Measure components and measure condition components are "
                                                        "mutually exclusive. A measure can have either components or condition components (if the ‘duty expression’ flag "
                                                        "is ‘mandatory' or 'optional') but not both.'", "", "", "", "430", "00", measure_sid)

    def create_error_report(self):
        fname = self.import_file + "_error.txt"
        path = os.path.join(self.ERROR_LOG_DIR, self.DBASE)
        path = os.path.join(path, fname)
        out = ""
        out += "Error log for file " + self.import_file + "\n"
        ln = len(out) - 1
        out += ("=" * ln) + "\n\n"

        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        out += "Load date / time: " + dt_string + "\n\n"

        out += "There are " + str(len(self.business_rule_violations)) + \
            " conformance / db errors, as follows:\n\n"

        cnt = 0
        for bvr in self.business_rule_violations:
            cnt += 1
            out += bvr.message + "\n"

        f = open(path, "w+")
        f.write(out)

    def register_import_start(self, xml_file):
        self.import_start_time = self.get_timestamp()
        sql = """
        INSERT INTO ml.import_files (import_file, import_started, status)
        VALUES  (%s, %s, 'Started')
        """
        params = [
            xml_file,
            self.import_start_time
        ]
        cur = self.conn.cursor()
        cur.execute(sql, params)
        self.conn.commit()

    def register_import_complete(self, xml_file):
        self.import_complete_time = self.get_timestamp()
        sql = """UPDATE ml.import_files SET import_completed = %s,
        status = 'Completed' WHERE import_file = %s"""
        params = [
            self.import_complete_time,
            xml_file
        ]
        cur = self.conn.cursor()
        cur.execute(sql, params)
        self.conn.commit()

    def larger(self, a, b):
        try:
            if a > b:
                return a
            else:
                return b
        except:
            return 0

    def get_scalar(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return (rows[0][0])

    def yes_or_no(self, question):
        reply = str(input(question + ' (y/n): ')).lower().strip()
        try:
            if reply[0] == 'y':
                return True
            if reply[0] == 'n':
                return False
            else:
                return self.yes_or_no(question)
        except:
            return self.yes_or_no(question)

    def document_xml(self, filename):
        tree = ET.parse(filename)
        self.root = tree.getroot()
        self.ns = {'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0',
                   'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0'}
        self.counts = {}
        self.add_count("additional.code")
        self.add_count("additional.code.description.period")
        self.add_count("additional.code.description")
        self.add_count("additional.code.type")
        self.add_count("additional.code.type.description")
        self.add_count("additional.code.type.measure.type")
        self.add_count("base.regulation")
        self.add_count("ceiling")
        self.add_count("certificate")
        self.add_count("certificate.description.period")
        self.add_count("certificate.description")
        self.add_count("certificate.type")
        self.add_count("certificate.type.description")
        self.add_count("complete.abrogation.regulation")
        self.add_count("duty.expression")
        self.add_count("duty.expression.description")
        self.add_count("explicit.abrogation.regulation")
        self.add_count("footnote")
        self.add_count("footnote.association.additional.code")
        self.add_count("footnote.association.goods.nomenclature")
        self.add_count("footnote.description.period")
        self.add_count("footnote.description")
        self.add_count("footnote.type")
        self.add_count("footnote.type.description")
        self.add_count("fts.regulation.action")
        self.add_count("full.temporary.stop.regulation")
        self.add_count("geographical.area")
        self.add_count("geographical.area.description.period")
        self.add_count("geographical.area.description")
        self.add_count("geographical.area.membership")
        self.add_count("goods.nomenclature")
        self.add_count("goods.nomenclature.indents")
        self.add_count("goods.nomenclature.description.period")
        self.add_count("goods.nomenclature.description")
        self.add_count("goods.nomenclature.origin")
        self.add_count("goods.nomenclature.successor")
        self.add_count("measure")
        self.add_count("measure.component")
        self.add_count("measure.condition")
        self.add_count("measure.condition.component")
        self.add_count("measure.excluded.geographical.area")
        self.add_count("footnote.association.measure")
        self.add_count("measure.partial.temporary.stop")
        self.add_count("measure.condition.code")
        self.add_count("measure.condition.code.description")
        self.add_count("measure.action")
        self.add_count("measure.action.description")
        self.add_count("measure.type")
        self.add_count("measure.type.description")
        self.add_count("measure.type.series")
        self.add_count("measure.type.series.description")
        self.add_count("measurement.unit")
        self.add_count("measurement.unit.description")
        self.add_count("measurement.unit.qualifier")
        self.add_count("measurement.unit.qualifier.description")
        self.add_count("measurement")
        self.add_count("modification.regulation")
        self.add_count("monetary.exchange.period")
        self.add_count("monetary.exchange.rate")
        self.add_count("monetary.unit")
        self.add_count("monetary.unit.description")
        self.add_count("prorogation.regulation")
        self.add_count("prorogation.regulation.action")
        self.add_count("quota.order.number")
        self.add_count("quota.order.number.origin")
        self.add_count("quota.order.number.origin.exclusions")
        self.add_count("quota.definition")
        self.add_count("quota.association")
        self.add_count("quota.blocking.period")
        self.add_count("quota.suspension.period")
        self.add_count("quota.extended.information")
        self.add_count("quota.balance.event")
        self.add_count("quota.unblocking.event")
        self.add_count("quota.critical.event")
        self.add_count("quota.exhaustion.event")
        self.add_count("quota.reopening.event")
        self.add_count("quota.unsuspension.event")
        self.add_count("quota.closed.and.transferred.event")
        self.add_count("regulation.group")
        self.add_count("regulation.group.description")
        self.add_count("regulation.role.type")
        self.add_count("regulation.role.type.description")
        self.add_count("regulation.replacement")

        ret = ""
        for key in self.counts:
            if key[0] != ">":
                ret += "\n"
            ret += key + " : " + str(self.counts[key]) + "\n"
        return ret

    def add_count(self, node):
        if node == "transaction":
            xpath = ".//env:transaction"
            count = len(self.root.findall(xpath, self.ns))
            self.counts[node] = count
        else:
            # all records
            xpath = "./env:transaction/env:app.message/oub:transmission/oub:record/oub:" + node
            count = len(self.root.findall(xpath, self.ns))
            if count > 0:
                self.counts[node.upper()] = count

            # inserted records
            xpath = "./env:transaction/env:app.message/oub:transmission/oub:record/oub:" + \
                node + "/../[oub:update.type='3']"
            count = len(self.root.findall(xpath, self.ns))
            if count > 0:
                self.counts[">  " + node + " - inserted records"] = count

            # updated records
            xpath = "./env:transaction/env:app.message/oub:transmission/oub:record/oub:" + \
                node + "/../[oub:update.type='1']"
            count = len(self.root.findall(xpath, self.ns))
            if count > 0:
                self.counts[">  " + node + " - updated records"] = count

            # deleted records
            xpath = "./env:transaction/env:app.message/oub:transmission/oub:record/oub:" + \
                node + "/../[oub:update.type='2']"
            count = len(self.root.findall(xpath, self.ns))
            if count > 0:
                self.counts[">  " + node + " - deleted records"] = count

    def record_business_rule_violation(self, id, msg, operation, transaction_id, message_id, record_code, sub_record_code, pk):
        bvr = business_rule_violation(
            id, msg, operation, transaction_id, message_id, record_code, sub_record_code, pk)
        self.business_rule_violations.append(bvr)
        print(bcolors.OKGREEN)
        print(bvr.message)
        sys.exit()

    def to_nice_time(self, dt):
        r = dt[0:4] + "-" + dt[4:6] + "-" + dt[6:8] + " " + \
            dt[9:11] + ":" + dt[11:13] + ":" + dt[13:15]
        return (r)

    def rollback(self):
        sql = "select * from ml.clear_data(%s, %s)"
        params = [
            self.to_nice_time(self.import_start_time),
            self.import_file
        ]

        cur = self.conn.cursor()
        cur.execute(sql, params)
        self.conn.commit()

    # Functions required for rule checks for import scripts
    def get_footnote_types(self):
        sql = "select footnote_type_id from footnote_types where validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_footnotes(self):
        sql = "select footnote_type_id || footnote_id as code from footnotes order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_footnote_type_descriptions(self):
        sql = "select footnote_type_id from footnote_type_descriptions order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_additional_code_types(self):
        sql = "select additional_code_type_id from additional_code_types where validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_meursing_table_plans(self):
        sql = "select meursing_table_plan_id from meursing_table_plans where validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_used_non_meursing_additional_codes(self):
        sql = "select distinct additional_code_type_id from additional_codes order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_used_additional_codes(self):
        sql = "select distinct additional_code_sid from measures where additional_code_sid is not null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_used_footnote_codes(self):
        sql = """select distinct (footnote_type_id || footnote_id) as code from footnote_association_measures
        union select distinct (footnote_type || footnote_id) as code from footnote_association_goods_nomenclatures
        union select distinct (footnote_type || footnote_id) as code from footnote_association_erns
        union select distinct (footnote_type_id || footnote_id) as code from footnote_association_additional_codes
        union select distinct (footnote_type || footnote_id) as code from footnote_association_meursing_headings
        order by 1;"""
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_additional_code_types_used_in_erns(self):
        sql = "select distinct additional_code_type from export_refund_nomenclatures order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_additional_code_types_mapped_to_measure_types(self):
        sql = "select distinct additional_code_type_id from additional_code_type_measure_types where validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_languages(self):
        sql = "select distinct language_id from languages where validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_additional_code_type_descriptions(self):
        sql = """select actp.additional_code_type_id
        from additional_code_type_descriptions actp, additional_code_types act
        where actp.additional_code_type_id = act.additional_code_type_id
        and act.validity_end_date is null
        order by 1
        ;"""
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_certificates(self):
        sql = "select distinct certificate_type_code || certificate_code as code from certificates order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_used_certificates(self):
        sql = "select distinct certificate_type_code || certificate_code as code from measure_conditions order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_certificate_types(self):
        sql = "select certificate_type_code from certificate_types where validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_certificate_type_descriptions(self):
        sql = "select certificate_type_code from certificate_type_descriptions order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_regulation_groups(self):
        sql = "select regulation_group_id from regulation_groups where validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_all_regulation_groups(self):
        sql = """select regulation_group_id, validity_start_date, coalesce(validity_end_date, TO_DATE('2999-12-31', 'YYYY-MM-DD'))
        as validity_end_date from regulation_groups order by 1;"""
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = list(rows)
        return (my_list)

    def get_related_modification_regulations(self, base_regulation_id):
        sql = """select modification_regulation_id, modification_regulation_role, validity_start_date, validity_end_date
        from modification_regulations where base_regulation_id = %s
        order by validity_start_date;"""
        params = [
            base_regulation_id
        ]
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        my_list = list(rows)
        return (my_list)

    def get_existing_related_origins(self, quota_order_number_sid, geographical_area_id):
        sql = """select quota_order_number_origin_sid, validity_start_date, coalesce(validity_end_date, TO_DATE('2999-12-31', 'YYYY-MM-DD')) as validity_end_date
        from quota_order_number_origins
        where quota_order_number_sid = %s
        and geographical_area_id = %s
        order by validity_start_date desc;"""
        params = [
            quota_order_number_sid,
            geographical_area_id
        ]
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        my_list = list(rows)
        return (my_list)

    def get_quota_order_number(self, quota_order_number_sid):
        sql = """select validity_start_date, coalesce(validity_end_date, TO_DATE('2999-12-31', 'YYYY-MM-DD')) as validity_end_date,
        quota_order_number_id from quota_order_numbers where quota_order_number_sid = %s limit 1;"""
        params = [
            quota_order_number_sid
        ]
        cur = self.conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        my_list = list(row)
        return (my_list)

    def get_geographical_area(self, geographical_area_sid):
        sql = """select validity_start_date, coalesce(validity_end_date, TO_DATE('2999-12-31', 'YYYY-MM-DD')) as validity_end_date,
        geographical_area_id, geographical_code from geographical_areas where geographical_area_sid = %s limit 1;"""
        params = [
            geographical_area_sid
        ]
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchone()
        my_list = list(rows)
        return (my_list)

    def get_geographical_area_from_origin(self, quota_order_number_origin_sid):
        sql = """select ga.validity_start_date, coalesce(ga.validity_end_date, TO_DATE('2999-12-31', 'YYYY-MM-DD')) as validity_end_date,
        ga.geographical_area_id, geographical_code
        from quota_order_number_origins qono, geographical_areas ga
        where qono.geographical_area_sid = ga.geographical_area_sid
        and qono.quota_order_number_origin_sid = %s
        limit 1;"""
        params = [
            quota_order_number_origin_sid
        ]
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchone()
        my_list = list(rows)
        return (my_list)

    def get_used_regulation_groups(self):
        sql = "select distinct regulation_group_id from base_regulations order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_used_regulation_roles(self):
        sql = """select distinct base_regulation_role from base_regulations
        union select distinct modification_regulation_role from modification_regulations
        union select distinct prorogation_regulation_role from prorogation_regulations
        union select distinct complete_abrogation_regulation_role from complete_abrogation_regulations
        union select distinct explicit_abrogation_regulation_role from explicit_abrogation_regulations
        union select distinct full_temporary_stop_regulation_role from full_temporary_stop_regulations
        order by 1;"""
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_geographical_area_groups(self):
        sql = "select geographical_area_sid from geographical_areas where geographical_code = '1' and validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_all_geographical_areas(self):
        sql = "select geographical_area_id from geographical_areas where validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_geographical_areas_with_dates(self):
        sql = "select geographical_area_id, validity_start_date from geographical_areas order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0] + "_" + row[1].strftime('%Y-%m-%d'))
        return (my_list)

    def get_similar_geographical_areas_with_dates(self, geographical_area_id):
        sql = """select geographical_area_sid, validity_start_date, coalesce(validity_end_date,
        TO_DATE('2999-12-31', 'YYYY-MM-DD')) from geographical_areas where geographical_area_id = %s order by 1;"""
        params = [
            geographical_area_id
        ]
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        my_list = list(rows)
        return (my_list)

    def get_similar_geographical_area_memberships_with_dates(self, geographical_area_group_sid, geographical_area_sid):
        sql = """select validity_start_date, coalesce(validity_end_date, TO_DATE('2999-12-31', 'YYYY-MM-DD')) as validity_end_date
        from geographical_area_memberships where geographical_area_group_sid = %s and geographical_area_sid = %s
        order by validity_end_date desc;"""
        params = [
            geographical_area_group_sid,
            geographical_area_sid,
        ]
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        my_list = list(rows)
        return (my_list)

    def get_all_goods_nomenclatures(self):
        sql = "select goods_nomenclature_item_id from goods_nomenclatures " \
            "where (validity_end_date is null or validity_end_date > %s) " \
            "and producline_suffix = '80' order by 1;"
        params = [
            self.critical_date_plus_one_string
        ]
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_all_geographical_area_sids(self):
        sql = "select geographical_area_sid from geographical_areas where validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_countries_regions(self):
        sql = "select geographical_area_sid from geographical_areas where geographical_code != '1' and validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_measurements(self):
        sql = """select measurement_unit_code, measurement_unit_qualifier_code
        from measurements where validity_end_date is null order by 1, 2;"""
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = list(rows)
        return (my_list)

    def get_base_regulations(self):
        sql = "select distinct base_regulation_role || base_regulation_id as code from base_regulations order by 1;"
        sql = "select distinct base_regulation_id as code from base_regulations order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_modification_regulations(self):
        sql = "select distinct modification_regulation_role || modification_regulation_id as code from modification_regulations order by 1;"
        sql = "select distinct modification_regulation_id as code from modification_regulations order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_all_regulations(self):
        sql = "select distinct modification_regulation_id as regulation_id, modification_regulation_role as regulation_role, validity_start_date, " \
            "validity_end_date from modification_regulations " \
            "union select distinct base_regulation_id as regulation_id, base_regulation_role as regulation_role, validity_start_date, validity_end_date from base_regulations " \
            "order by 1;"

        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        self.all_regulations_with_dates = []
        for row in rows:
            my_list.append(row[0])
            obj = []
            obj.append(row[0])
            obj.append(str(row[1]))
            obj.append(row[2])
            obj.append(row[3])
            self.all_regulations_with_dates.append(obj)
        return (my_list)

    def get_quota_order_numbers(self):
        sql = """select distinct on (quota_order_number_id)
        quota_order_number_id, quota_order_number_sid, validity_start_date, validity_end_date
        from quota_order_numbers qon
        order by 1, 3 desc"""
        sql = """select
        quota_order_number_id, quota_order_number_sid, validity_start_date, validity_end_date
        from quota_order_numbers qon
        order by 1, 3 desc"""
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        self.all_quota_order_numbers = []
        for row in rows:
            my_list.append(row[0])
            obj = []
            obj.append(row[0])
            obj.append(row[1])
            obj.append(row[2])
            obj.append(row[3])
            self.all_quota_order_numbers.append(obj)
        return (my_list)

    def get_quota_definitions(self):
        sql = """select quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date
        from quota_definitions order by quota_order_number_id, validity_start_date"""
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        self.all_quota_definitions = []
        for row in rows:
            my_list.append(row[0])
            obj = []
            obj.append(row[0])
            obj.append(row[1])
            obj.append(row[2])
            obj.append(row[3])
            self.all_quota_definitions.append(obj)
        return (my_list)

    def get_my_regulation(self, regulation_code):
        for item in self.all_regulations_with_dates:
            if item[0] == regulation_code:
                return (item)
                break

    def get_measure_types(self):
        sql = "select measure_type_id, validity_start_date, validity_end_date, order_number_capture_code from measure_types order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        self.all_measure_types = []
        for row in rows:
            my_list.append(row[0])
            obj = []
            obj.append(row[0])
            obj.append(row[1])
            obj.append(row[2])
            obj.append(row[3])
            self.all_measure_types.append(obj)
        return (my_list)

    def get_measure_type_series(self):
        sql = "select measure_type_series_id from measure_type_series where validity_end_date is null order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_used_measure_type_series(self):
        sql = "select distinct measure_type_series_id from measure_types order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        for row in rows:
            my_list.append(row[0])
        return (my_list)

    def get_duty_expressions(self):
        sql = """select duty_expression_id, validity_start_date, validity_end_date, duty_amount_applicability_code,
        measurement_unit_applicability_code, monetary_unit_applicability_code
        from duty_expressions where validity_end_date is null order by 1;"""
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        my_list = []
        self.all_duty_expressions = []
        for row in rows:
            my_list.append(row[0])
            obj = []
            obj.append(row[0])
            obj.append(row[1])
            obj.append(row[2])
            obj.append(row[3])
            obj.append(row[4])
            obj.append(row[5])
            self.all_duty_expressions.append(obj)
        return (my_list)

    def rule_FO04(self):
        print("Running business rule FO4 - Footnote description period must exist at the start of the footnote.")
        # Footnote description period must exist at the start of the footnote
        sql = "select footnote_type_id || footnote_id as code, validity_start_date from footnotes f order by 1, 2;"
        cur = self.conn.cursor()
        cur.execute(sql)
        footnote_list = cur.fetchall()

        sql = "select footnote_type_id || footnote_id as code, validity_start_date from footnote_description_periods fdp order by 1, 2;"
        cur = self.conn.cursor()
        cur.execute(sql)
        fdp_list = cur.fetchall()

        for item in footnote_list:
            code = item[0]
            validity_start_date = item[1]
            matched = False
            for item2 in fdp_list:
                code2 = item2[0]
                validity_start_date2 = item2[1]
                if code == code2 and validity_start_date == validity_start_date2:
                    matched = True
                    break
            if matched is False:
                # Business rule FO4
                self.record_business_rule_violation("FO4", "At least one description record is mandatory. The start date of the first "
                                                    "description period must be equal to the start date of the footnote. No two associated description periods may have the "
                                                    "same start date. The start date must be less than or equal to the end date of the footnote.", "", "", "", "200", "05", code)

    def rule_ACN5(self):
        print("Running business rule ACN5 - At least one additional code description is mandatory.")
        # At least one description is mandatory. The start date of the first description period must be equal to the start
        # date of the additional code. No two associated description periods may have the same start date.
        # The start date must be less than or equal to the end date of the additional code.
        sql = "select additional_code_sid, validity_start_date from additional_codes ac order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        ac_list = cur.fetchall()

        sql = "select additional_code_sid, validity_start_date from additional_code_description_periods acdp order by 1;"
        cur = self.conn.cursor()
        cur.execute(sql)
        acdp_list = cur.fetchall()

        for item in ac_list:
            sid = item[0]
            validity_start_date = item[1]
            matched = False
            for item2 in acdp_list:
                sid2 = item2[0]
                validity_start_date2 = item2[1]
                if sid == sid2 and validity_start_date == validity_start_date2:
                    matched = True
                    break
            if matched is False:
                # Business rule FO4
                self.record_business_rule_violation("ACN5", "At least one description record is mandatory. The start date of the first "
                                                    "description period must be equal to the start date of the additional code. No two associated description periods may have the "
                                                    "same start date. The start date must be less than or equal to the end date of the additional code.", "", "", "", "200", "05", sid)

    def rule_CE06(self):
        print("Running business rule CE6 - Certificate description period must exist at the start of the certificate.")
        # Certificate description period must exist at the start of the certificate
        sql = "select certificate_type_code || certificate_code as code, validity_start_date from certificates c order by 1, 2;"
        cur = self.conn.cursor()
        cur.execute(sql)
        certificate_list = cur.fetchall()

        sql = "select certificate_type_code || certificate_code as code, validity_start_date from certificate_description_periods cdp order by 1, 2;"
        cur = self.conn.cursor()
        cur.execute(sql)
        cdp_list = cur.fetchall()

        for item in certificate_list:
            code = item[0]
            validity_start_date = item[1]
            matched = False
            for item2 in cdp_list:
                code2 = item2[0]
                validity_start_date2 = item2[1]
                if code == code2 and validity_start_date == validity_start_date2:
                    matched = True
                    break
            if matched is False:
                self.record_business_rule_violation("CE6", "At least one description record is mandatory. The start date of the first "
                                                    "description period must be equal to the start date of the certificate. No two associated description periods for the "
                                                    "same certificate and language may have the same start date. The validity period of the certificate must span the validity "
                                                    "period of the certificate description.", "", "", "", "250", "10", code)

    def rule_GA3(self):
        print("Running business rule GA3 - Geographical area description period must exist at the start of the Geographical area.")
        # Geographical area description period must exist at the start of the Geographical area
        sql = "select geographical_area_id, validity_start_date from geographical_areas order by 1, 2;"
        cur = self.conn.cursor()
        cur.execute(sql)
        ga_list = cur.fetchall()

        sql = "select geographical_area_id, validity_start_date from geographical_area_description_periods cdp order by 1, 2;"
        cur = self.conn.cursor()
        cur.execute(sql)
        gadp_list = cur.fetchall()

        for item in ga_list:
            code = item[0]
            validity_start_date = item[1]
            matched = False
            for item2 in gadp_list:
                code2 = item2[0]
                validity_start_date2 = item2[1]
                if code == code2 and validity_start_date == validity_start_date2:
                    matched = True
                    break
            if matched is False:
                # Business rule GA3
                self.record_business_rule_violation("GA3", "At least one description record is mandatory. The start date of the first description period must "
                                                    "be equal to the start date of the geographical area. No two associated description periods for the same geographical area and language "
                                                    "may have the same start date. The validity period of the geographical area must span the validity period of the geographical area "
                                                    "description.", "", "", "", "250", "10", code)

    def rule_ME43(self):
        print("Running business rule ME43 - The same duty expression can only be used once with the same measure.")
        # Business rule ME43 The same duty expression can only be used once with the same measure.
        sql = "select * from components_per_measure_sid_and_duty_expression where component_count > 1 order by measure_sid;"
        cur = self.conn.cursor()
        cur.execute(sql)
        my_list = cur.fetchall()
        for item in my_list:
            measure_sid = item[0]
            self.record_business_rule_violation(
                "ME43", "The same duty expression can only be used once with the same measure.", "", "", "", "430", "05", measure_sid)

    def get_update_string(self, operation):
        if operation == "D":
            return "delete"
        elif operation == "C":
            return "create"
        elif operation == "U":
            return "update"

    def get_loading_message(self, update_type, object_description, value):
        operation = ""
        if update_type == "1":  # UPDATE
            operation = "U"
            self.doprint("Updating " + object_description + " " + str(value))
        elif update_type == "2":  # DELETE
            operation = "D"
            self.doprint("Deleting " + object_description + " " + str(value))
        elif update_type == "3":  # INSERT
            operation = "C"
            self.doprint("Creating " + object_description + " " + str(value))

        return operation
