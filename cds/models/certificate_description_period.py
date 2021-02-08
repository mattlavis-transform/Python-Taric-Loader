import sys

import common.globals as g
from common.database import Database
from cds.models.master import Master
from cds.models.certificate_description import CertificateDescription


class CertificateDescriptionPeriod(Master):
    def __init__(self, elem, certificate_type_code, certificate_code, import_file):
        Master.__init__(self, elem)
        self.certificate_type_code = certificate_type_code
        self.certificate_code = certificate_code
        self.certificate_description_period_sid = Master.process_null(elem.find("sid"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        operation_date = g.app.get_timestamp()

        # Insert the certificate description period
        sql = """
        insert into certificate_description_periods_oplog
        (certificate_description_period_sid, certificate_type_code, certificate_code, validity_start_date, validity_end_date,
        operation, operation_date, created_at, filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = [
            self.certificate_description_period_sid,
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

        # Create new certificate descriptions
        for elem in elem.findall('.//certificateDescription'):
            CertificateDescription(elem, self.certificate_description_period_sid, self.certificate_type_code, self.certificate_code, import_file)
