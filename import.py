# Import custom libraries
import sys
import common.globals as g

# Get database into which to import from 1st argument
if len(sys.argv) > 1:
    dbase = sys.argv[1]
    if "tariff_" not in dbase:
        dbase = "tariff_" + dbase
else:
    sys.exit()

# Get name of file to import
if len(sys.argv) > 2:
    filename = sys.argv[2]
else:
    sys.exit()

# print (dbase)
# sys.exit()
app = g.app
app.DBASE = dbase
app.get_config()
app.import_xml(filename)
