import os
import string
import random
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

COV = None
if os.environ.get('FLASK_COVERAGE'):
    import coverage
    COV = coverage.coverage(branch=True, include='app/*')
    COV.start()

import sys
import click
from flask_migrate import Migrate, upgrade
from app import create_app, db
from app.models import Location, LocationTree, PoliceStation

app = create_app(os.getenv('FLASK_CONFIG') or 'default')
migrate = Migrate(app, db)


@app.shell_context_processor
def make_shell_context():
    return {'app': app, 'db': db, 'Location': Location}


@app.cli.command("initdb")
def initdb():
    def id_generator(size=12, chars=string.ascii_lowercase + string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    country = Location.query.filter_by(name='Malawi', level=1).all()
    if country:
        print("Database Already Initialized")
        return

    print("Database Initialization Starting.............!")

    db.session.add(LocationTree(name='Malawi Administrative Divisions'))
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
    print("Database Initialization Complete!")
