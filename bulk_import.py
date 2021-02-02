# Import custom libraries
import sys
import common.globals as g
from common.taric_file import TaricFile

# Get database into which to import from 1st argument
if __name__ == "__main__":
    # First argument is the database into which to import the data
    if len(sys.argv) > 1:
        dbase = sys.argv[1]
    else:
        print("Define the database into which to import the Taric XML")
        sys.exit()

    # Connect to the database
    g.app.DBASE = dbase
    g.app.connect()

    # Import the files
    for i in range(10, 233):
        file = "TGB20" + str(i).zfill(3) + ".xml"
        print(file)
        g.data_file = TaricFile(file)
        g.data_file.import_xml()
