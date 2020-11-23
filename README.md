# Taric Import
## Implementation steps

- Create and activate a virtual environment, e.g.

  `python3 -m venv venv/`
  `source venv/bin/activate`

- Install necessary Python modules 

  - elementpath==2.0.4
  - psycopg2==2.8.6
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

The config.json file in the /config subfolder allows you to make 

`{`

​    `"critical_date": "2020-01-31",`

​    `"debug": 1,`

​    `"perform_taric_validation": 0,`

​    `"show_progress": 1`

`}`