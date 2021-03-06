# Import custom libraries
import sys
import common.globals as g
from taric.taric_file import TaricFile
from cds.cds_file import CdsFile


# Get database into which to import from 1st argument
if __name__ == "__main__":
    # First argument is the database into which to import the data
    if len(sys.argv) > 1:
        g.app.DBASE = sys.argv[1]
    else:
        print("\n\nPlease define the database into which to import the Taric XML\n\n")
        sys.exit()

    # Second argument is the name of the file to import
    if len(sys.argv) > 2:
        import_filename = sys.argv[2]
        g.app.set_data_file_source()

        if g.app.import_type == "CDS":
            g.data_file = CdsFile(import_filename)
            # g.app.connect()
            g.data_file.import_xml()
        else:
            g.data_file = TaricFile(import_filename)
            g.app.connect()
            g.data_file.import_xml()
    else:
        print("\n\nPlease identify which Taric XML file to load\n\n")
        sys.exit()
