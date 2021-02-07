import xml.etree.ElementTree as ET
import xmlschema
import sys
import os


from common.bcolors import bcolors
from taric.profile.profile_10000_footnote_type import profile_10000_footnote_type
from taric.profile.profile_10005_footnote_type_description import profile_10005_footnote_type_description
from taric.profile.profile_11000_certificate_type import profile_11000_certificate_type
from taric.profile.profile_11005_certificate_type_description import profile_11005_certificate_type_description
from taric.profile.profile_12000_additional_code_type import profile_12000_additional_code_type
from taric.profile.profile_12005_additional_code_type_description import profile_12005_additional_code_type_description
from taric.profile.profile_13000_language import profile_13000_language
from taric.profile.profile_13005_language_description import profile_13005_language_description
from taric.profile.profile_14000_measure_type_series import profile_14000_measure_type_series
from taric.profile.profile_14005_measure_type_series_description import profile_14005_measure_type_series_description
from taric.profile.profile_15000_regulation_group import profile_15000_regulation_group
from taric.profile.profile_15005_regulation_group_description import profile_15005_regulation_group_description
from taric.profile.profile_16000_regulation_role_type import profile_16000_regulation_role_type
from taric.profile.profile_16005_regulation_role_type_description import profile_16005_regulation_role_type_description
from taric.profile.profile_17000_publication_sigle import profile_17000_publication_sigle
from taric.profile.profile_20000_footnote import profile_20000_footnote
from taric.profile.profile_20005_footnote_description_period import profile_20005_footnote_description_period
from taric.profile.profile_20010_footnote_description import profile_20010_footnote_description
from taric.profile.profile_20500_certificate import profile_20500_certificate
from taric.profile.profile_20505_certificate_description_period import profile_20505_certificate_description_period
from taric.profile.profile_20510_certificate_description import profile_20510_certificate_description
from taric.profile.profile_21000_measurement_unit import profile_21000_measurement_unit
from taric.profile.profile_21005_measurement_unit_description import profile_21005_measurement_unit_description
from taric.profile.profile_21500_measurement_unit_qualifier import profile_21500_measurement_unit_qualifier
from taric.profile.profile_21505_measurement_unit_qualifier_description import profile_21505_measurement_unit_qualifier_description
from taric.profile.profile_22000_measurement import profile_22000_measurement
from taric.profile.profile_22500_monetary_unit import profile_22500_monetary_unit
from taric.profile.profile_22505_monetary_unit_description import profile_22505_monetary_unit_description
from taric.profile.profile_23000_duty_expression import profile_23000_duty_expression
from taric.profile.profile_23005_duty_expression_description import profile_23005_duty_expression_description
from taric.profile.profile_23500_measure_type import profile_23500_measure_type
from taric.profile.profile_23505_measure_type_description import profile_23505_measure_type_description
from taric.profile.profile_24000_additional_code_type_measure_type import profile_24000_additional_code_type_measure_type
from taric.profile.profile_24500_additional_code import profile_24500_additional_code
from taric.profile.profile_24505_additional_code_description_period import profile_24505_additional_code_description_period
from taric.profile.profile_24510_additional_code_description import profile_24510_additional_code_description
from taric.profile.profile_24515_footnote_association_additional_code import profile_24515_footnote_association_additional_code
from taric.profile.profile_25000_geographical_area import profile_25000_geographical_area
from taric.profile.profile_25005_geographical_area_description_period import profile_25005_geographical_area_description_period
from taric.profile.profile_25010_geographical_area_description import profile_25010_geographical_area_description
from taric.profile.profile_25015_geographical_area_membership import profile_25015_geographical_area_membership
from taric.profile.profile_27000_goods_nomenclature_group import profile_27000_goods_nomenclature_group
from taric.profile.profile_27005_goods_nomenclature_group_description import profile_27005_goods_nomenclature_group_description
from taric.profile.profile_27500_complete_abrogation_regulation import profile_27500_complete_abrogation_regulation
from taric.profile.profile_28000_explicit_abrogation_regulation import profile_28000_explicit_abrogation_regulation
from taric.profile.profile_28500_base_regulation import profile_28500_base_regulation
from taric.profile.profile_29000_modification_regulation import profile_29000_modification_regulation
from taric.profile.profile_29500_prorogation_regulation import profile_29500_prorogation_regulation
from taric.profile.profile_29505_prorogation_regulation_action import profile_29505_prorogation_regulation_action
from taric.profile.profile_30000_full_temporary_stop_regulation import profile_30000_full_temporary_stop_regulation
from taric.profile.profile_30005_fts_regulation_action import profile_30005_fts_regulation_action
from taric.profile.profile_30500_regulation_replacement import profile_30500_regulation_replacement
from taric.profile.profile_32000_meursing_table_plan import profile_32000_meursing_table_plan
from taric.profile.profile_32500_meursing_heading import profile_32500_meursing_heading
from taric.profile.profile_32505_meursing_heading_text import profile_32505_meursing_heading_text
from taric.profile.profile_32510_footnote_association_meursing_heading import profile_32510_footnote_association_meursing_heading
from taric.profile.profile_33000_meursing_subheading import profile_33000_meursing_subheading
from taric.profile.profile_34000_meursing_additional_code import profile_34000_meursing_additional_code
from taric.profile.profile_34005_meursing_table_cell_component import profile_34005_meursing_table_cell_component
from taric.profile.profile_35000_measure_condition_code import profile_35000_measure_condition_code
from taric.profile.profile_35005_measure_condition_code_description import profile_35005_measure_condition_code_description
from taric.profile.profile_35500_measure_action import profile_35500_measure_action
from taric.profile.profile_35505_measure_action_description import profile_35505_measure_action_description
from taric.profile.profile_36000_quota_order_number import profile_36000_quota_order_number
from taric.profile.profile_36010_quota_order_number_origin import profile_36010_quota_order_number_origin
from taric.profile.profile_36015_quota_order_number_origin_exclusion import profile_36015_quota_order_number_origin_exclusion
from taric.profile.profile_37000_quota_definition import profile_37000_quota_definition
from taric.profile.profile_37005_quota_association import profile_37005_quota_association
from taric.profile.profile_37010_quota_blocking_period import profile_37010_quota_blocking_period
from taric.profile.profile_37015_quota_suspension_period import profile_37015_quota_suspension_period
from taric.profile.profile_37500_quota_balance_event import profile_37500_quota_balance_event
from taric.profile.profile_37505_quota_unblocking_event import profile_37505_quota_unblocking_event
from taric.profile.profile_37510_quota_critical_event import profile_37510_quota_critical_event
from taric.profile.profile_37515_quota_exhaustion_event import profile_37515_quota_exhaustion_event
from taric.profile.profile_37520_quota_reopening_event import profile_37520_quota_reopening_event
from taric.profile.profile_37525_quota_unsuspension_event import profile_37525_quota_unsuspension_event
from taric.profile.profile_37530_quota_closed_and_balance_transferred_event import profile_37530_quota_closed_and_balance_transferred_event
from taric.profile.profile_40000_goods_nomenclature import profile_40000_goods_nomenclature
from taric.profile.profile_40005_goods_nomenclature_indent import profile_40005_goods_nomenclature_indent
from taric.profile.profile_40010_goods_nomenclature_description_period import profile_40010_goods_nomenclature_description_period
from taric.profile.profile_40015_goods_nomenclature_description import profile_40015_goods_nomenclature_description
from taric.profile.profile_40020_footnote_association_goods_nomenclature import profile_40020_footnote_association_goods_nomenclature
from taric.profile.profile_40025_nomenclature_group_membership import profile_40025_nomenclature_group_membership
from taric.profile.profile_40035_goods_nomenclature_origin import profile_40035_goods_nomenclature_origin
from taric.profile.profile_40040_goods_nomenclature_successor import profile_40040_goods_nomenclature_successor
from taric.profile.profile_41000_export_refund_nomenclature import profile_41000_export_refund_nomenclature
from taric.profile.profile_41005_export_refund_nomenclature_indent import profile_41005_export_refund_nomenclature_indent
from taric.profile.profile_41010_export_refund_nomenclature_description_period import profile_41010_export_refund_nomenclature_description_period
from taric.profile.profile_41015_export_refund_nomenclature_description import profile_41015_export_refund_nomenclature_description
from taric.profile.profile_41020_footnote_association_ern import profile_41020_footnote_association_ern
from taric.profile.profile_43000_measure import profile_43000_measure
from taric.profile.profile_43005_measure_component import profile_43005_measure_component
from taric.profile.profile_43010_measure_condition import profile_43010_measure_condition
from taric.profile.profile_43011_measure_condition_component import profile_43011_measure_condition_component
from taric.profile.profile_43015_measure_excluded_geographical_area import profile_43015_measure_excluded_geographical_area
from taric.profile.profile_43020_footnote_association_measure import profile_43020_footnote_association_measure
from taric.profile.profile_43025_measure_partial_temporary_stop import profile_43025_measure_partial_temporary_stop
from taric.profile.profile_44000_monetary_exchange_period import profile_44000_monetary_exchange_period
from taric.profile.profile_44005_monetary_exchange_rate import profile_44005_monetary_exchange_rate
import common.globals as g
from taric.business_rule_violation import business_rule_violation


class TaricFile(object):
    def __init__(self, import_file):
        self.business_rule_violations = []
        self.import_file = import_file
        g.app.set_data_file_source()
        self.import_path_and_file = os.path.join(g.app.IMPORT_FOLDER, self.import_file)
        a = 1

    def import_xml(self):
        self.duty_measure_list = []

        g.app.print_to_terminal(
            "Preparing to import file " + self.import_file + " into database " + g.app.DBASE, False)

        if g.app.prompt:
            ret = g.app.yes_or_no("Do you want to continue?")
            if not (ret) or ret in ("n", "N", "No"):
                sys.exit()

        g.app.load_data_sets_for_validation()
        self.check_already_loaded()
        g.app.create_log_file(self.import_file)

        # Load file
        ET.register_namespace(
            'oub', 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0')
        ET.register_namespace(
            'env', 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0')
        try:
            tree = ET.parse(self.import_path_and_file)
        except:
            print(
                "The selected file could not be found or is not a valid, well-formed XML file")
            sys.exit(0)
        root = tree.getroot()

        self.register_import_start(self.import_file)

        for oTransaction in root.findall('.//env:transaction', g.app.namespaces):
            for omsg in oTransaction.findall('.//env:app.message', g.app.namespaces):
                record_code = omsg.find(
                    ".//oub:record.code", g.app.namespaces).text
                sub_record_code = omsg.find(
                    ".//oub:subrecord.code", g.app.namespaces).text
                update_type = omsg.find(
                    ".//oub:update.type", g.app.namespaces).text
                transaction_id = omsg.find(
                    ".//oub:transaction.id", g.app.namespaces).text
                message_id = omsg.attrib["id"]

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

        g.app.log_handle.close()

        if g.app.perform_taric_validation is True:
            # Post load checks
            self.rule_FO04()
            self.rule_ACN5()
            self.rule_CE06()
            self.rule_GA3()
            self.rule_ME40()
            self.rule_ME43()

        # Register the load
        g.app.register_import_complete(self.import_file)
        print(bcolors.ENDC)
        if len(self.business_rule_violations) > 0:
            print("File failed to load - rolling back")
            self.rollback()
            self.create_error_report()
        else:
            g.app.print_to_terminal("Load complete with no errors", False)

    def validate(self):
        # Needs work to simplify
        msg = "Validating the final XML file against the Taric 3 schema"
        g.app.print_to_terminal(
            "Validating the final XML file against the Taric 3 schema")
        schema_path = os.path.join(g.app.SCHEMA_DIR, "envelope.xsd")
        my_schema = xmlschema.XMLSchema(schema_path)
        try:
            if my_schema.is_valid(self.import_path_and_file):
                g.app.print_to_terminal("The file validated successfully")
                success = True
            else:
                g.app.print_to_terminal("The file did not validate")
                success = False
        except:
            g.app.print_to_terminal(
                "The file did not validate and crashed the validator")
            success = False

        if success is False:
            try:
                my_schema.validate(self.import_path_and_file)
            except:
                g.app.print_to_terminal(
                    "The file did not validate and crashed the validator")

    def check_already_loaded(self):
        # Check that this file has not already been imported
        sql = "SELECT import_file FROM utils.import_files WHERE import_file = %s"
        params = [
            self.import_file
        ]
        cur = g.app.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        if len(rows) > 0:
            g.app.show_progress = True
            g.app.print_to_terminal("File " + self.import_file + " has already been imported - Aborting now\n", False)
            sys.exit()

    def register_import_start(self, xml_file):
        self.import_start_time = g.app.get_timestamp()
        sql = """
        INSERT INTO utils.import_files (import_file, import_started, status)
        VALUES  (%s, %s, 'Started')
        """
        params = [
            xml_file,
            self.import_start_time
        ]
        cur = g.app.conn.cursor()
        cur.execute(sql, params)
        g.app.conn.commit()

    def record_business_rule_violation(self, id, msg, operation, transaction_id, message_id, record_code, sub_record_code, pk):
        bvr = business_rule_violation(
            id, msg, operation, transaction_id, message_id, record_code, sub_record_code, pk)
        self.business_rule_violations.append(bvr)
        print(bcolors.OKGREEN)
        print(bvr.message)
        sys.exit()

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
            cur = g.app.conn.cursor()
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

    def rule_FO04(self):
        print("Running business rule FO4 - Footnote description period must exist at the start of the footnote.")
        # Footnote description period must exist at the start of the footnote
        sql = "select footnote_type_id || footnote_id as code, validity_start_date from footnotes f order by 1, 2;"
        cur = g.app.conn.cursor()
        cur.execute(sql)
        footnote_list = cur.fetchall()

        sql = "select footnote_type_id || footnote_id as code, validity_start_date from footnote_description_periods fdp order by 1, 2;"
        cur = g.app.conn.cursor()
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
        cur = g.app.conn.cursor()
        cur.execute(sql)
        ac_list = cur.fetchall()

        sql = "select additional_code_sid, validity_start_date from additional_code_description_periods acdp order by 1;"
        cur = g.app.conn.cursor()
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
        cur = g.app.conn.cursor()
        cur.execute(sql)
        certificate_list = cur.fetchall()

        sql = "select certificate_type_code || certificate_code as code, validity_start_date from certificate_description_periods cdp order by 1, 2;"
        cur = g.app.conn.cursor()
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
        cur = g.app.conn.cursor()
        cur.execute(sql)
        ga_list = cur.fetchall()

        sql = "select geographical_area_id, validity_start_date from geographical_area_description_periods cdp order by 1, 2;"
        cur = g.app.conn.cursor()
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
        cur = g.app.conn.cursor()
        cur.execute(sql)
        my_list = cur.fetchall()
        for item in my_list:
            measure_sid = item[0]
            self.record_business_rule_violation(
                "ME43", "The same duty expression can only be used once with the same measure.", "", "", "", "430", "05", measure_sid)
