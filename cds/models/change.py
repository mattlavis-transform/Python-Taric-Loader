import sys

import common.globals as g


class Change(object):
    def __init__(self, sid, id, change_type):
        self.sid = sid
        self.id = id
        self.change_type = change_type
