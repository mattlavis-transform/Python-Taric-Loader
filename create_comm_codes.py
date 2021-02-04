# Import custom libraries
import sys
import common.globals as g


if __name__ == "__main__":
    if len(sys.argv) > 1:
        g.app.DBASE = sys.argv[1]
    else:
        print("Define the database into which to import the Taric XML")
        sys.exit()

    g.app.connect()
    g.app.create_commodity_extract()
