import psycopg2
import sys
import os
from os import system, name, path
import csv
from dotenv import load_dotenv
from datetime import datetime

from common.log import log
from common.database import Database
from common.classification import Classification
from taric.taric_file import TaricFile


class application(object):
    def __init__(self):
        self.clear()
        load_dotenv('.env')
        self.debug = os.getenv('DEBUG')
        self.password = os.getenv('PASSWORD')
        self.perform_taric_validation = self.num_to_bool(os.getenv('PERFORM_TARIC_VALIDATION'))
        self.show_progress = self.num_to_bool(os.getenv('SHOW_PROGRESS'))
        self.prompt = self.num_to_bool(os.getenv('PROMPT'))
        self.IMPORT_FOLDER = os.getenv('IMPORT_FOLDER')
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.BASE_DIR = os.path.normpath(os.path.join(self.BASE_DIR, ".."))
        self.CSV_DIR = os.path.join(self.BASE_DIR, "csv")
        self.LOG_DIR = os.path.join(self.BASE_DIR, "log")
        self.IMPORT_LOG_DIR = os.path.join(self.LOG_DIR, "import")
        self.ERROR_LOG_DIR = os.path.join(self.LOG_DIR, "errors")

        self.SCHEMA_DIR = os.path.normpath(os.path.join(self.BASE_DIR, ".."))
        self.SCHEMA_DIR = os.path.join(self.SCHEMA_DIR, "xsd")

        self.DBASE = None
        self.EU_DATABASES = os.getenv('EU_DATABASES').split(",")
        self.UK_DATABASES = os.getenv('UK_DATABASES').split(",")
        self.import_type = None

        self.namespaces = {'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0',
                           'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0', }  # add more as needed
        self.message_id = 1
        self.message_count = 0
        self.load_errors = []
        self.get_vscode_debug_mode()

    def set_data_file_source(self):
        if self.DBASE in self.EU_DATABASES:
            self.IMPORT_FOLDER = os.path.join(self.IMPORT_FOLDER, "EU")
            self.import_type = "Taric"

        elif self.DBASE in self.UK_DATABASES:
            self.IMPORT_FOLDER = os.path.join(self.IMPORT_FOLDER, "CDS")
            self.import_type = "CDS"

        else:
            print("Please specify a valid database name")
            sys.exit()

    def num_to_bool(self, num):
        num = int(num)
        if num == 0:
            return False
        else:
            return True

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                "dbname=" + self.DBASE + " user=postgres password=" + self.password)
            self.print_to_terminal(
                "Connected to database '{0}'".format(self.DBASE), False)
        except:
            self.print_to_terminal(
                "Could not connect to database '{0}'".format(self.DBASE), False)
            sys.exit()

    def load_data_sets_for_validation(self):
        if self.perform_taric_validation:
            self.get_measure_types_that_require_components()
            self.load_classification_trees()
            self.all_regulations = self.get_all_regulations()
            self.geographical_area_sids = self.get_all_geographical_area_sids()
            self.geographical_areas = self.get_all_geographical_areas()
            self.quota_order_numbers = self.get_quota_order_numbers()
            self.quota_definitions = self.get_quota_definitions()
            self.goods_nomenclatures = self.get_all_goods_nomenclatures()
            self.duty_expressions = self.get_duty_expressions()

    def get_deleted_goods_nomenclatures(self):
        self.deleted_goods_nomenclatures = []
        sql = """select distinct goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix
        from utils.deleted_goods_nomenclatures
        order by goods_nomenclature_item_id, productline_suffix"""
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            obj = [row[0], row[1], row[2]]
            self.deleted_goods_nomenclatures.append(obj)

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
        if self.show_progress or include_indent is True:
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

    def print_only(self, s):
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
                            c = Classification(row[0], row[1], int(
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
        if self.perform_taric_validation is True:
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
                    self.measure_types_that_require_components_list.append(
                        rw[0])

    def get_vscode_debug_mode(self):
        ret = sys.gettrace()
        if ret is None:
            self.vscode_debug_mode = False
        else:
            self.vscode_debug_mode = True

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

    def register_import_complete(self, xml_file):
        return
        self.import_complete_time = self.get_timestamp()
        sql = """UPDATE utils.import_files SET import_completed = %s,
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

    def to_nice_time(self, dt):
        r = dt[0:4] + "-" + dt[4:6] + "-" + dt[6:8] + " " + \
            dt[9:11] + ":" + dt[11:13] + ":" + dt[13:15]
        return (r)

    def rollback(self):
        sql = "select * from utils.clear_data(%s, %s)"
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
            datetime.strftime(datetime.now(), '%Y-%m-%d')
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

    def create_log_file(self, import_file):
        self.IMPORT_LOG_FILE = os.path.join(
            self.IMPORT_LOG_DIR, "log_" + import_file)
        self.IMPORT_LOG_FILE = self.IMPORT_LOG_FILE.replace("xml", "txt")
        self.log_handle = open(self.IMPORT_LOG_FILE, "w")

    def create_commodity_extract(self, which="eu"):
        print("Creating commodity code extract")
        d = datetime.now()
        d2 = d.strftime('%Y-%m-%d')
        self.classifications = []
        for i in range(0, 10):
            chapter = str(i) + "%"
            sql = "select * from utils.goods_nomenclature_export_new('" + chapter + "', '" + d2 + "') order by 2, 3"
            print("Getting complete commodity code list for codes beginning with " + str(i))
            d = Database()
            rows = d.run_query(sql)
            for row in rows:
                self.validity_start_date = str(row[0])
                classification = Classification(
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10]
                )
                self.classifications.append(classification)

        filename = os.path.join(self.CSV_DIR, which + "_commodities_" + d2 + ".csv")

        f = open(filename, "w+")
        field_names = '"SID","Commodity code","Product line suffix","Start date","End date","Indentation","End line","Description"\n'
        f.write(field_names)
        for item in self.classifications:
            f.write(item.extract_row())
        f.close()

    def format_date(self, d):
        if d is None:
            return ""
        elif d == "":
            return ""
        else:
            return d[8:10] + "/" + d[5:7] + "/" + d[0:4]
