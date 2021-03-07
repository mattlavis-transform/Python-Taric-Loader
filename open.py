import os
import sys
import common.globals as g


if len(sys.argv) < 2:
    sys.exit()

which = sys.argv[1].upper()

if which in ("EU", "TARIC"):
    IMPORT_FOLDER = os.path.join(g.app.IMPORT_FOLDER, "EU")
else:
    IMPORT_FOLDER = os.path.join(g.app.IMPORT_FOLDER, "CDS")

# systems.get(os.name, os.startfile)(foldername)
os.system('open "%s"' % IMPORT_FOLDER)
