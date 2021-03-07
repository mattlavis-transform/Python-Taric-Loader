import sys
import common.globals as g
from common.database import Database

if len(sys.argv) > 1:
    g.app.DBASE = sys.argv[1].lower()
    if g.app.DBASE == "xi":
        g.app.DBASE = "tariff_xi_production"
    elif g.app.DBASE == "uk":
        g.app.DBASE = "tariff_uk_production"
else:
    print("Define the database into which to import the Taric XML")
    sys.exit()

sql = "REFRESH MATERIALIZED VIEW utils.materialized_measures_real_end_dates"
d = Database()
d.run_query(sql)
