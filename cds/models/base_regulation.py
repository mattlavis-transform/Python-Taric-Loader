import sys

import common.globals as g
from common.database import Database
from cds.models.change import Change
from cds.models.master import Master
from cds.models.footnote_type_description import FootnoteTypeDescription


class BaseRegulation(Master):
    def __init__(self, elem, import_file, transform_only=False):
        Master.__init__(self, elem)
        self.approved_flag = Master.process_null(elem.find("approvedFlag"))
        self.base_regulation_id = Master.process_null(elem.find("baseRegulationId"))
        self.community_code = Master.process_null(elem.find("communityCode"))
        self.effective_end_date = Master.process_null(elem.find("effectiveEndDate"))
        self.information_text = Master.process_null(elem.find("informationText"))
        self.officialjournal_number = Master.process_null(elem.find("officialjournalNumber"))
        self.officialjournal_page = Master.process_null(elem.find("officialjournalPage"))
        self.published_date = Master.process_null(elem.find("publishedDate"))
        self.replacement_indicator = Master.process_null(elem.find("replacementIndicator"))
        self.stopped_flag = Master.process_null(elem.find("stoppedFlag"))
        self.validity_start_date = Master.process_date(elem.find("validityStartDate"))
        self.validity_end_date = Master.process_date(elem.find("validityEndDate"))
        self.regulation_group_id = Master.process_null(elem.find("regulationGroup/regulationGroupId"))
        self.regulation_role_type_id = Master.process_null(elem.find("regulationRoleType/regulationRoleTypeId"))

        operation_date = g.app.get_timestamp()

        g.app.print_only("Running operation {0} on base regulation {1}.".format(self.operation, self.base_regulation_id))

        if transform_only is False:
            # Insert the base regulation
            sql = """
            insert into base_regulations_oplog
            (approved_flag, base_regulation_id, community_code,
            effective_end_date, information_text, officialjournal_number, officialjournal_page,
            published_date, replacement_indicator, stopped_flag,
            validity_start_date, validity_end_date,
            regulation_group_id, base_regulation_role,
            operation, operation_date, created_at, filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                self.approved_flag,
                self.base_regulation_id,
                self.community_code,
                self.effective_end_date,
                self.information_text,
                self.officialjournal_number,
                self.officialjournal_page,
                self.published_date,
                self.replacement_indicator,
                self.stopped_flag,
                self.validity_start_date,
                self.validity_end_date,
                self.regulation_group_id,
                self.regulation_role_type_id,
                self.operation,
                operation_date,
                operation_date,
                import_file
            ]
            d = Database()
            d.run_query(sql, params)
