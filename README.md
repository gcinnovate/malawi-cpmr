CPMR
=====
The UNICEF Malawi Child Protection Mobile Reporting (CPMR) dashboard backend application

How to Set up the development server
1. Clone the CPMR application from Github

$ git clone https:/github.com/gcinnovate/malawi-cpmr.git

$ cd malawi-cpmr

2. Create and activate virtual environment

$ python3 -m venv env

$ . env/bin/activate

3. Install Dependencies

$ pip install -r requirements.txt

3. Configuration
The configurations for the CPMR application are made using environment variables.
Refer to the config.py file for the environment variables to set.

$ export FLASK_APP=cmpr.py

$ export FLASK_CONFIG=default

$ export DEV_DATABASE_URL=postgresql://postgres:postgres@localhost/cpmr

4. Migrate database

$ flask db upgrade

5. Populate database with locations and police stations

$ flask initdb
This runs the initdb command in the cpmr.py application script to initialize database with some data

6. Running the application

$ flask run

$ celery worker -A celery_worker.celery --loglevel=info
