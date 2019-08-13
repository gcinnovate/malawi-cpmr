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

4. Configuration
The configurations for the CPMR application are made using environment variables.
Refer to the config.py file for the environment variables to set.

$ export FLASK_APP=cmpr.py

$ export FLASK_CONFIG=default

$ export DEV_DATABASE_URL=postgresql://postgres:postgres@localhost/cpmr

5. Migrate database

$ flask db upgrade

6. Populate database with locations and police stations

$ flask initdb
This runs the initdb command in the cpmr.py application script to initialize database with some data

7. There are two commands available to help load some test data.

$ flask load_test_data

$ flask load_test_data2

You can use the --help option on each of the commands to see some arguments you can pass to the commands.

8. To creating views to use as data sources, we use the following flask command

$ flask create-views

9. Creating an admin user

$ flask create-user

10. Running the application

$ flask run

$ celery worker -A celery_worker.celery --loglevel=info
