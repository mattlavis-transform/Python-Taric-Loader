# Import custom libraries
import sys
import common.globals as g

# Get database into which to import from 1st argument
if __name__ == "__main__":
    # First argument is the database into which to import the data
    if len(sys.argv) > 1:
        dbase = sys.argv[1]
    else:
        print("Define the database into which to import the Taric XML")
        sys.exit()

    # Second argument is the name of the file to import
    if len(sys.argv) > 2:
        filename = sys.argv[2]
    else:
        print("Identify which Taric XML file to load")
        sys.exit()

    app = g.app
    app.DBASE = dbase
    app.get_config()
    app.connect()
    app.import_xml(filename)
