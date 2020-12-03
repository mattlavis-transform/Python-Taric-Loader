# Taric Import
## Implementation steps

- Create and activate a virtual environment, e.g.

  `python3 -m venv venv/`
  `source venv/bin/activate`

- Install necessary Python modules 

  - autopep8==1.5.4
  - elementpath==2.0.4
  - et-xmlfile==1.0.1
  - jdcal==1.4.1
  - numpy==1.19.4
  - openpyxl==3.0.5
  - pandas==1.1.4
  - psycopg2==2.8.6
  - pycodestyle==2.6.0
  - python-dateutil==2.8.1
  - python-dotenv==0.15.0
  - pytz==2020.4
  - sh==1.14.1
  - six==1.15.0
  - toml==0.10.2
  - xmlschema==1.3.1

  via `pip3 install -r requirements.txt`

- Create the PostgreSQL database locally, then run the data load script

  `structure.sql`

  e.g. via `psql -U postgres -d tariff_empty -a -f db/structure.sql`

## Usage

Imports full and incremental Taric XML files.

Python 3 application which runs on command line using two parameters:

- Parameter 1 = name of PostgreSQL database into which to import the data
- Parameter 2 = name of the incremental Taric 3 file to import into the database

By default, all files should be placed in the /import folder relative to the root directory

## Configuration switches

CRITICAL_DATE=yyyy-mm-dd
DEBUG=0|1
PASSWORD=xx
PERFORM_TARIC_VALIDATION=0|1
SHOW_PROGRESS=0|1
PROMPT=0|1
