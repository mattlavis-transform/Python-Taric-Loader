from datetime import datetime


class Master(object):
    def __init__(self, elem):
        try:
            self.operation = elem.find("metainfo/opType").text
        except:
            self.operation = ""
        try:
            self.national = 1 if elem.find("metainfo/origin").text == "N" else 0
        except:
            self.national = 0

    @staticmethod
    def process_null(elem):
        if elem is None:
            return None
        else:
            return elem.text

    @staticmethod
    def process_date(elem):
        if elem is None:
            return None
        else:
            s = elem.text
            s = s.replace("T00:00:00", "")
            d = datetime.strptime(s, "%Y-%m-%d")
            return d
