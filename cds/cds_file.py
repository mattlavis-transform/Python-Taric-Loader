import xml.etree.ElementTree as ET
import xmlschema
import sys
import os
from common.database import Database
from common.bcolors import bcolors
import common.globals as g

from cds.models.footnote_type import FootnoteType
from cds.models.certificate_type import CertificateType
from cds.models.additional_code_type import AdditionalCodeType
from cds.models.measure_type import MeasureType
from cds.models.footnote import Footnote
from cds.models.certificate import Certificate
from cds.models.additional_code import AdditionalCode
from cds.models.geographical_area import GeographicalArea
from cds.models.base_regulation import BaseRegulation
from cds.models.quota_order_number import QuotaOrderNumber
from cds.models.quota_definition import QuotaDefinition
from cds.models.goods_nomenclature import GoodsNomenclature
from cds.models.measure import Measure


class CdsFile(object):
    def __init__(self, import_file):
        if import_file == "next":
            self.latest_file = self.latest()
            path = os.walk(g.app.IMPORT_FOLDER)
            file_array = []
            for root, directories, files in path:
                for file in files:
                    file_array.append(file)
            file_array.sort(reverse=False)
            for file in file_array:
                if ".xml" in file:
                    if file > self.latest_file:
                        import_file = file
                        break

        self.business_rule_violations = []
        self.import_file = import_file
        self.import_path_and_file = os.path.join(
            g.app.IMPORT_FOLDER, self.import_file)
        self.initialise_variables()

    def latest(self, silent=True):
        sql = "SELECT import_file FROM utils.import_files order by 1 desc limit 1"
        d = Database()
        rows = d.run_query(sql)
        if len(rows) > 0:
            if silent is False:
                print("The last file to have been imported is", rows[0][0])
            return rows[0][0]
        else:
            return ""

    def initialise_variables(self):
        self.base_regulations = []
        self.measures = []
        self.goods_nomenclatures = []
        self.quota_order_numbers = []
        self.quota_definitions = []

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
        try:
            tree = ET.parse(self.import_path_and_file)
        except:
            print(
                "The selected file could not be found or is not a valid, well-formed XML file")
            sys.exit(0)
        root_node = tree.getroot()

        self.register_import_start(self.import_file)

        # Get footnote types
        for elem in root_node.findall('.//findFootnoteTypeByDatesResponse/FootnoteType'):
            FootnoteType(elem, self.import_file)

        # Get certificate types
        for elem in root_node.findall('.//findCertificateTypeByDatesResponse/CertificateType'):
            CertificateType(elem, self.import_file)

        # Get additional code types
        for elem in root_node.findall('.//findAdditionalCodeTypeByDatesResponse/AdditionalCodeType'):
            AdditionalCodeType(elem, self.import_file)

        # Get measure types
        for elem in root_node.findall('.//findMeasureTypeByDatesResponse/MeasureType'):
            MeasureType(elem, self.import_file)

        # Get footnotes
        for elem in root_node.findall('.//findFootnoteByDatesResponse/Footnote'):
            Footnote(elem, self.import_file)

        # Get certificates
        for elem in root_node.findall('.//findCertificateByDatesResponse/Certificate'):
            Certificate(elem, self.import_file)

        # Get additional codes
        for elem in root_node.findall('.//findAdditionalCodeByDatesResponse/AdditionalCode'):
            AdditionalCode(elem, self.import_file)

        # Get geographical areas
        for elem in root_node.findall('.//findGeographicalAreaByDatesResponse/GeographicalArea'):
            GeographicalArea(elem, self.import_file)

        # Get base regulations
        for elem in root_node.findall('.//findBaseRegulationByDatesResponseHistory/BaseRegulation'):
            BaseRegulation(elem, self.import_file)

        # Get quota order numbers
        for elem in root_node.findall('.//findQuotaOrderNumberByDatesResponseHistory/QuotaOrderNumber'):
            QuotaOrderNumber(elem, self.import_file)

        # Get quota order definitions
        for elem in root_node.findall('.//findQuotaDefinitionByDatesResponseHistory/QuotaDefinition'):
            QuotaDefinition(elem, self.import_file)

        # Get goods nomenclatures
        for elem in root_node.findall('.//findGoodsNomenclatureByDatesResponse/GoodsNomenclature'):
            GoodsNomenclature(elem, self.import_file)

        # Get measures
        for elem in root_node.findall('.//findMeasureByDatesResponseHistory/Measure'):
            Measure(elem, self.import_file)

        # Register the load
        g.app.register_import_complete(self.import_file)
        print(bcolors.ENDC)
        g.app.print_to_terminal("Load complete with no errors", False)

    def transform_xml(self):
        self.duty_measure_list = []

        g.app.print_to_terminal(
            "Preparing to import file " + self.import_file + " into database " + g.app.DBASE, False)

        if g.app.prompt:
            ret = g.app.yes_or_no("Do you want to continue?")
            if not (ret) or ret in ("n", "N", "No"):
                sys.exit()

        g.app.load_data_sets_for_validation()
        self.check_already_loaded()
        try:
            tree = ET.parse(self.import_path_and_file)
        except:
            print(
                "The selected file could not be found or is not a valid, well-formed XML file")
            sys.exit(0)
        root_node = tree.getroot()

        self.register_import_start(self.import_file)

        # Get footnote types
        for elem in root_node.findall('.//findFootnoteTypeByDatesResponse/FootnoteType'):
            FootnoteType(elem, self.import_file, transform_only=True)

        # Get certificate types
        for elem in root_node.findall('.//findCertificateTypeByDatesResponse/CertificateType'):
            CertificateType(elem, self.import_file, transform_only=True)

        # Get additional code types
        for elem in root_node.findall('.//findAdditionalCodeTypeByDatesResponse/AdditionalCodeType'):
            AdditionalCodeType(elem, self.import_file, transform_only=True)

        # Get measure types
        for elem in root_node.findall('.//findMeasureTypeByDatesResponse/MeasureType'):
            MeasureType(elem, self.import_file, transform_only=True)

        # Get footnotes
        for elem in root_node.findall('.//findFootnoteByDatesResponse/Footnote'):
            Footnote(elem, self.import_file, transform_only=True)

        # Get certificates
        for elem in root_node.findall('.//findCertificateByDatesResponse/Certificate'):
            Certificate(elem, self.import_file, transform_only=True)

        # Get additional codes
        for elem in root_node.findall('.//findAdditionalCodeByDatesResponse/AdditionalCode'):
            AdditionalCode(elem, self.import_file, transform_only=True)

        # Get geographical areas
        for elem in root_node.findall('.//findGeographicalAreaByDatesResponse/GeographicalArea'):
            GeographicalArea(elem, self.import_file, transform_only=True)

        # Get base regulations
        for elem in root_node.findall('.//findBaseRegulationByDatesResponseHistory/BaseRegulation'):
            BaseRegulation(elem, self.import_file, transform_only=True)

        # Get quota order numbers
        for elem in root_node.findall('.//findQuotaOrderNumberByDatesResponseHistory/QuotaOrderNumber'):
            QuotaOrderNumber(elem, self.import_file, transform_only=True)

        # Get quota order definitions
        for elem in root_node.findall('.//findQuotaDefinitionByDatesResponseHistory/QuotaDefinition'):
            QuotaDefinition(elem, self.import_file, transform_only=True)

        # Get goods nomenclatures
        for elem in root_node.findall('.//findGoodsNomenclatureByDatesResponse/GoodsNomenclature'):
            GoodsNomenclature(elem, self.import_file, transform_only=True)

        # Get measures
        for elem in root_node.findall('.//findMeasureByDatesResponseHistory/Measure'):
            Measure(elem, self.import_file, transform_only=True)

        # Register the load
        g.app.register_import_complete(self.import_file)
        print(bcolors.ENDC)
        g.app.print_to_terminal("Load complete with no errors", False)

    def check_already_loaded(self):
        # Check that this file has not already been imported
        sql = "SELECT import_file FROM utils.import_files WHERE import_file = %s"
        params = [
            self.import_file
        ]
        d = Database()
        rows = d.run_query(sql, params)
        if len(rows) > 0:
            g.app.show_progress = True
            g.app.print_to_terminal(
                "File " + self.import_file + " has already been imported - Aborting now\n", False)
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
        d = Database()
        d.run_query(sql, params)
