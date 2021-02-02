# Import custom libraries
import sys
import common.globals as g
from common.taric_file import TaricFile


# Get database into which to import from 1st argument
if __name__ == "__main__":
    # First argument is the database into which to import the data
    if len(sys.argv) > 1:
        g.app.DBASE = sys.argv[1]
    else:
        print("Define the database into which to import the Taric XML")
        sys.exit()

    # Second argument is the name of the file to import
    if len(sys.argv) <= 2:
        print("Identify which Taric XML file to load")
        sys.exit()
    else:
        g.data_file = TaricFile(sys.argv[2])
        g.app.connect()
        g.data_file.import_xml()
