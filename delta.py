# Import custom libraries
import sys
import common.globals as g


if __name__ == "__main__":
    # Args are:
    # 1 = the database
    # 2 = key date in format yyyy-mm-dd
    # 3 = period: day | week

    if len(sys.argv) > 1:
        g.app.DBASE = sys.argv[1]
        if len(sys.argv) > 2:
            g.app.change_date = sys.argv[2]
            if len(sys.argv) > 3:
                g.app.change_period = sys.argv[3].lower()
            else:
                print("\nDefine the change period\n")
                sys.exit()
        else:
            print("\nDefine the change date\n")
            sys.exit()
    else:
        print("\nDefine the database into which to import the Taric XML\n")
        sys.exit()

    g.app.connect()
    g.app.create_delta()
