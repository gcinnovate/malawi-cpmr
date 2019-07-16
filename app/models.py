from datetime import datetime
from . import db
from sqlalchemy_mptt.mixins import BaseNestedSets
from sqlalchemy.dialects.postgresql import JSONB
# from sqlalchemy.ext.declarative import declared_attr

import string
import random


def id_generator(size=12, chars=string.ascii_lowercase + string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


class TimeStampMixin(object):
    created = db.Column(db.DateTime(), nullable=False, default=datetime.utcnow)
    updated = db.Column(db.DateTime(), nullable=False, default=datetime.utcnow)


class LocationTree(db.Model):
    __tablename__ = 'locationtree'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(), index=True)


class Location(db.Model, BaseNestedSets):
    __tablename__ = 'locations'
    id = db.Column(db.Integer, primary_key=True)
    tree_id = db.Column(db.Integer, db.ForeignKey('locationtree.id'))
    code = db.Column(db.String(64), index=True, unique=True)
    name = db.Column(db.String(64), index=True)


class PoliceStation(db.Model):
    __tablename__ = 'police_stations'
    id = db.Column(db.Integer, primary_key=True)
    district_id = db.Column(db.Integer, db.ForeignKey('locations.id'))
    name = db.Column(db.String(64), index=True)


class FlowData(db.Model, TimeStampMixin):
    __tablename__ = 'flow_data'
    id = db.Column(db.Integer, primary_key=True)
    region = db.Column(db.Integer, db.ForeignKey('locations.id'))
    district = db.Column(db.Integer, db.ForeignKey('locations.id'))
    station = db.Column(db.Integer, db.ForeignKey('police_stations.id'))
    report_type = db.Column(db.String(), index=True)
    msisdn = db.Column(db.String(), index=True)
    month = db.Column(db.String(), index=True)
    values = db.Column(JSONB)
