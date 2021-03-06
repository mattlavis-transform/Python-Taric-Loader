import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.certificate_description_period import CertificateDescriptionPeriod


class Certificate(Master):
    def __init__(self, elem, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.certificate_type_code = Master.process_null(elem.find("certificateType/certificateTypeCode"))
        self.certificate_code = Master.process_null(elem.find("certificateCode"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        operation_date = g.app.get_timestamp()

        g.app.print_only("Running operation {0} on certificate {1}.".format(self.operation, str(self.certificate_code)))

        # Insert the certificate
        if transform_only is False:
            sql = """
            insert into certificates_oplog
            (certificate_type_code, certificate_code, validity_start_date, validity_end_date,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.certificate_type_code,
                self.certificate_code,
                self.validity_start_date,
                self.validity_end_date,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)

            # Delete any certificate descriptions
            sql = """
            delete from certificate_descriptions_oplog
            where certificate_type_code = %s and certificate_code = %s
            """
            params = [self.certificate_type_code, self.certificate_code]
            d = Database()
            d.run_query(sql, params)

            # Delete any certificate description periods
            sql = """
            delete from certificate_description_periods_oplog
            where certificate_type_code = %s and certificate_code = %s
            """
            params = [self.certificate_type_code, self.certificate_code]
            d = Database()
            d.run_query(sql, params)

            # Create new certificate description periods
            for elem in elem.findall('.//certificateDescriptionPeriod'):
                CertificateDescriptionPeriod(elem, self.certificate_type_code, self.certificate_code, import_file)
