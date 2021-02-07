class log(object):
    def __init__(self, record_code, sub_record_code, update_type_string, sid, filename, desc):
        self.record_code = record_code
        self.sub_record_code = sub_record_code
        self.update_type_string = update_type_string
        self.sid = sid
        self.filename = filename
        self.desc = desc
