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
            # Starts as text in format 2018-12-15T04:11:14
            # Needs to get converted into a date with the time removed
            pos = s.find("T")
            if pos > 0:
                s = s[0:pos]
            # s = s.replace("T00:00:00", "")
            d = datetime.strptime(s, "%Y-%m-%d")
            return d
