import os
# import sys
import string
import random
import click
from dotenv import load_dotenv
from flask_migrate import Migrate, upgrade
from app import create_app, db, redis_client
from app.models import (
    Location, LocationTree, PoliceStation, User, Role,
    FlowData, JusticeCourt, SummaryCases)
from datetime import datetime
from flask import current_app
from sqlalchemy.sql import text
from getpass import getpass
from config import INDICATOR_NAME_MAPPING

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

COV = None
if os.environ.get('FLASK_COVERAGE'):
    import coverage
    COV = coverage.coverage(branch=True, include='app/*')
    COV.start()


app = create_app(os.getenv('FLASK_CONFIG') or 'default')
migrate = Migrate(app, db)


@app.before_first_request
def before_first_request_func():
    locs = Location.query.filter_by(level=3).all()
    districts = {}
    for l in locs:
        districts[l.name] = {'id': l.id, 'parent_id': l.parent_id}
    redis_client.districts = districts

    stations = PoliceStation.query.all()
    police_stations = {}
    for s in stations:
        police_stations[s.name] = s.id
    redis_client.police_stations = police_stations

    print("This function will run once")


@app.shell_context_processor
def make_shell_context():
    return dict(app=app, db=db, User=User, Role=Role)


@app.cli.command("initdb")
def initdb():
    def id_generator(size=12, chars=string.ascii_lowercase + string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    Role.insert_roles()
    country = Location.query.filter_by(name='Malawi', level=1).all()
    if country:
        click.echo("Database Already Initialized")
        return

    click.echo("Database Initialization Starting.............!")

    db.session.add(LocationTree(name='Malawi Administrative Divisions'))
    db.session.commit()
    # Add country
    db.session.add(Location(name='Malawi', code=id_generator(), tree_id=1))  # Country
    # Add the regions
    db.session.add_all(
        [
            Location(name='Central', code=id_generator(), parent_id=1, tree_id=1),  # 2
            Location(name='Eastern', code=id_generator(), parent_id=1, tree_id=1),  # 3
            Location(name='Northern', code=id_generator(), parent_id=1, tree_id=1),  # 4
            Location(name='Southern', code=id_generator(), parent_id=1, tree_id=1),  # 5
        ]
    )

    # Central = 2, Eastern = 3, Northern = 4, Southern = 5
    regions_data = {
        '2': [
            'Dedza', 'Dowa', 'Kasungu', 'Lilongwe', 'Mchinji',
            'Nkhotakota', 'Ntcheu', 'Ntchisi', 'Salima'],
        '3': ['Balaka', 'Machinga', 'Mangochi', 'Zomba'],
        '4': ['Chitipa', 'Karonga', 'Likoma', 'Mzimba', 'Nkhata Bay', 'Rumphi'],
        '5': [
            'Blantyre', 'Chikwawa', 'Chiradzulu', 'Mulanje', 'Mwanza', 'Neno', 'Nsanje',
            'Phalombe', 'Thyolo']
    }
    for k, v in regions_data.items():
        for val in v:
            db.session.add(Location(name=val, code=id_generator(), parent_id=k, tree_id=1))
    db.session.commit()

    # Each district name corresponds to a Police Station in Malawi
    districts = Location.query.filter_by(level=3).all()
    for d in districts:
        db.session.add(PoliceStation(name=d.name, district_id=d.id))

    blantyre = Location.query.filter_by(name='Blantyre', level=3).all()[0]
    db.session.add(PoliceStation(name='Chileka', district_id=blantyre.id))
    db.session.add(PoliceStation(name='Limbe', district_id=blantyre.id))

    mzimba = Location.query.filter_by(name='Mzimba', level=3).all()[0]
    db.session.add(PoliceStation(name='Mzuzu', district_id=mzimba.id))

    nkhotakota = Location.query.filter_by(name='Nkhotakota', level=3).all()[0]
    db.session.add(PoliceStation(name='Nkhuriga', district_id=nkhotakota.id))

    dowa = Location.query.filter_by(name='Dowa', level=3).all()[0]
    db.session.add(PoliceStation(name='Mponero', district_id=dowa.id))

    lilongwe = Location.query.filter_by(name='Lilongwe', level=3).all()[0]
    db.session.add(PoliceStation(name='Kanengo', district_id=lilongwe.id))

    db.session.commit()
    click.echo("Database Initialization Complete!")


@app.cli.command("load_test_data")
@click.option('--report', '-r', default='pvsu')
@click.option('--start-year', '-s', default=2016)
@click.option('--end-year', '-e', default=2020)
def load_test_data(report, start_year, end_year):
    from config import INDICATOR_CATEGORY_MAPPING, INDICATOR_THRESHOLD
    # print(report)
    police_stations = PoliceStation.query.all()
    year = datetime.now().year
    for y in range(start_year, end_year):
        for m in range(1, 13):
            if y == year and m > datetime.now().month - 1:
                continue
            click.echo("{0}-{1:02}".format(y, m))
            for p in police_stations:
                district_id = p.district_id
                region_id = Location.query.filter_by(id=district_id).first().parent_id
                month_str = "{0}-{1:02}".format(y, m)
                total_cases = 0
                boys_total = 0
                girls_total = 0
                men_total = 0
                women_total = 0
                values = {}
                for k, v in INDICATOR_CATEGORY_MAPPING.get(report).items():
                    indcators_total = 0
                    for ind in v:
                        field = "{0}_{1}".format(ind, k)
                        val = random.choice(range(INDICATOR_THRESHOLD[k]))
                        values[field] = val
                        indcators_total += val
                        if ind == 'boys':
                            boys_total += val
                        elif ind == 'girls':
                            girls_total += val
                        elif ind == 'men':
                            men_total += val
                        elif ind == 'women':
                            women_total += val
                    values[k] = indcators_total
                    total_cases += indcators_total
                values['total_cases'] = total_cases
                if report == 'pvsu':
                    values['boys_total'] = boys_total
                    values['girls_total'] = girls_total
                    values['men_total'] = men_total
                    values['women_total'] = women_total
                db.session.add(FlowData(
                    region=region_id, district=district_id, station=p.id, month=month_str,
                    year=y, report_type=report, values=values))
                click.echo(values)
            db.session.commit()


@app.cli.command("load_test_data2")
@click.option('--report', '-r', default='ncjf')
@click.option('--start-year', '-s', default=2016)
@click.option('--end-year', '-e', default=2020)
def load_test_data2(report, start_year, end_year):
    from config import INDICATOR_CATEGORY_MAPPING, INDICATOR_THRESHOLD
    print(report)
    justice_courts = JusticeCourt.query.all()
    year = datetime.now().year
    for y in range(start_year, end_year):
        for m in range(1, 13):
            if y == year and m > datetime.now().month - 1:
                continue
            click.echo("{0}-{1:02}".format(y, m))
            for p in justice_courts:
                district_id = p.district_id
                region_id = Location.query.filter_by(id=district_id).first().parent_id
                month_str = "{0}-{1:02}".format(y, m)
                total_cases = 0
                values = {}
                for k, v in INDICATOR_CATEGORY_MAPPING.get(report).items():
                    indcators_total = 0
                    if not v:  # if indicator has no sub categories!
                        field = "{0}".format(k)
                        val = random.choice(range(INDICATOR_THRESHOLD[k]))
                        values[field] = val
                        continue

                    for ind in v:
                        field = "{0}_{1}".format(ind, k)
                        val = random.choice(range(INDICATOR_THRESHOLD[k]))
                        values[field] = val
                        indcators_total += val
                    values[k] = indcators_total
                    total_cases += indcators_total
                values['total_cases'] = total_cases
                db.session.add(FlowData(
                    region=region_id, district=district_id, court=p.id, month=month_str,
                    year=y, report_type=report, values=values))
                click.echo(values)
            db.session.commit()


@app.cli.command()
def deploy():
    """Run deployment tasks."""
    # migrate database to latest revision
    upgrade()


@app.cli.command("create-user")
def createuser():
    username = input("Enter Username: ")
    email = input("Enter Email: ")
    password = getpass()
    cpass = getpass("Confirm Password: ")
    assert password == cpass
    u = User(username=username, email=email)
    u.password = cpass
    db.session.add(u)
    db.session.commit()
    u.confirmed = True
    db.session.commit()
    click.echo("User added!")


@app.cli.command("create-views")
def create_views():
    with current_app.open_resource('../views.sql') as f:
        # print(f.read())
        click.echo("Gonna create views")
        db.session.execute(text(f.read().decode('utf8')))
        db.session.commit()
        click.echo("Done creating views")


@app.cli.command("refresh-pvsu-casetypes")
def refresh_pvsu_casetypes():
    results = db.engine.execute("SELECT * FROM pvsu_casetypes_regional_view order by year desc")
    # print(results.keys())
    records = []
    for row in results:
        month = row['month']
        year = row['year']
        region_id = row['region_id']
        for k in results.keys():
            if k in ('month', 'year', 'region_id'):
                continue
            casetype, cases = (k, row[k])
            records.append((casetype, cases, month, year, region_id))

    print(records)
    for r in records:
        summary = SummaryCases.query.filter_by(
            casetype=INDICATOR_NAME_MAPPING.get(r[0], r[0]), month=r[2], year=r[3],
            region=r[4], report_type='pvsu', summary_for='region', summary_slug='types').first()
        if summary:
            summary.value = r[1]
        else:
            s = SummaryCases(
                casetype=INDICATOR_NAME_MAPPING.get(r[0], r[0]), value=r[1], month=r[2], year=r[3],
                region=r[4], report_type='pvsu', summary_for='region', summary_slug='types')
            db.session.add(s)
        db.session.commit()

    # Load data for pvsu regional demography
    results = db.engine.execute("SELECT * FROM pvsu_cases_demographics_regional_view order by year desc")
    records = []
    for row in results:
        month = row['month']
        year = row['year']
        region_id = row['region_id']
        for k in results.keys():
            if k in ('month', 'year', 'region_id'):
                continue
            casetype, cases = (k, row[k])
            records.append((casetype, cases, month, year, region_id))

    for r in records:
        summary = SummaryCases.query.filter_by(
            casetype=INDICATOR_NAME_MAPPING.get(r[0], r[0]), month=r[2], year=r[3], region=r[4],
            report_type='pvsu', summary_for='region', summary_slug='demography').first()
        if summary:
            summary.value = r[1]
        else:
            s = SummaryCases(
                casetype=INDICATOR_NAME_MAPPING.get(r[0], r[0]), value=r[1], month=r[2], year=r[3],
                region=r[4], report_type='pvsu', summary_for='region', summary_slug='demography')
            db.session.add(s)
        db.session.commit()
