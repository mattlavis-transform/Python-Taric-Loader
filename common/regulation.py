from datetime import datetime
import os


class regulation(object):
    def __init__(self, regulation_id, information_text, reg_type):
        self.regulation_id = regulation_id
        self.information_text = information_text.replace(",", ";")
        self.reg_type = reg_type
        self.cleanse()

    def cleanse(self):
        self.information_text = self.information_text.replace(",", ";")
        self.information_text = self.information_text.replace('"', "")
        self.information_text = self.information_text.replace("'", "")
        self.information_text = self.information_text.replace("\n", " ")
        self.information_text = self.information_text.replace("  ", " ")
        self.information_text = self.information_text.strip()
