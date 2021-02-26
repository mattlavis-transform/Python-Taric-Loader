# Import custom libraries
import sys
import common.globals as g
from taric.taric_file import TaricFile
from cds.cds_file import CdsFile


# Get database into which to import from 1st argument
if __name__ == "__main__":
    # First argument is the database into which to import the data
    if len(sys.argv) > 0:
        g.app.DBASE = sys.argv[1]
    else:
        print("Define the database into which to import the Taric XML")
        sys.exit()

    g.app.set_data_file_source()

    if g.app.import_type == "CDS":
        g.data_file = CdsFile("")
        # g.app.connect()
        g.data_file.latest(silent=False)
    else:
        g.data_file = TaricFile("")
        g.app.connect()
        g.data_file.latest(silent=False)
