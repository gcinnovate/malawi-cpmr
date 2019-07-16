CPMR
=====
The UNICEF Malawi Child Protection Mobile Reporting (CPMR) dashboard backend application

How to initialize the database
1. Clone the CPMR application from Github

$ git clone https:/github.com/gcinnovate/malawi-cpmr.git
$ cd malawi-cpmr

1. Set environment variables
export FLASK_APP=cmpr.py
export FLASK_CONFIG=default
export DEV_DATABASE_URL=postgresql://postgres:postgres@localhost/cpmr

2. Migrate database

$ flask db upgrade

3. Populate database with locations and police stations

$ flask initdb
This runs the initdb command in the cpmr.py application script
