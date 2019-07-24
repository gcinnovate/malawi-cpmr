from datetime import datetime
from sqlalchemy_mptt.mixins import BaseNestedSets
from sqlalchemy.dialects.postgresql import JSONB
# from sqlalchemy.ext.declarative import declared_attr
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from flask import current_app, url_for
from flask_login import UserMixin, AnonymousUserMixin
from . import db, login_manager

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
    """Our MPTT Hierarchical structure for the Locations Tree"""
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
    """To hold flow data from RapidPro"""
    __tablename__ = 'flow_data'
    id = db.Column(db.Integer, primary_key=True)
    region = db.Column(db.Integer, db.ForeignKey('locations.id'))
    district = db.Column(db.Integer, db.ForeignKey('locations.id'))
    station = db.Column(db.Integer, db.ForeignKey('police_stations.id'))
    report_type = db.Column(db.String(), index=True)
    msisdn = db.Column(db.String(), index=True)
    month = db.Column(db.String(), index=True)
    year = db.Column(db.Integer, index=True)
    values = db.Column(JSONB)

    def to_json(self):
        json_flowdata = {
            'url': url_for('api.get_flowdata', id=self.id),
            'region': self.region.name,
            'district': self.district.name,
        }
        return json_flowdata


class Indicator(db.Model):
    """ Will hold the recorgnized indicators(variables) to be read from RapidPro"""
    __tablename__ = 'indicators'
    id = db.Column(db.Integer, primary_key=True)
    report_type = db.Column(db.String(), index=True)
    category = db.Column(db.String(), index=True)
    slug = db.Column(db.String(), index=True)
    description = db.Column(db.String())
    created = db.Column(db.DateTime(), nullable=False, default=datetime.utcnow)
    updated = db.Column(db.DateTime(), nullable=False, default=datetime.utcnow)

    def to_json(self):
        json_indicator = {
            'report_type': self.report_type,
            'slug': self.slug,
            'category': self.category,
            'description': self.description
        }
        return json_indicator


class Permission:
    VIEW = 1
    EDIT = 2
    ADMIN = 4


class Role(db.Model):
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), unique=True)
    default = db.Column(db.Boolean, default=False, index=True)
    permissions = db.Column(db.Integer)
    users = db.relationship('User', backref='role', lazy='dynamic')

    def __repr__(self):
        return '<Role %r>' % self.name

    def __init__(self, **kwargs):
        super(Role, self).__init__(**kwargs)
        if self.permissions is None:
            self.permissions = 0

    @staticmethod
    def insert_roles():
        roles = {
            'Viewer': [Permission.VIEW],
            'Editor': [Permission.VIEW, Permission.EDIT],
            'Administrator': [Permission.VIEW, Permission.EDIT, Permission.ADMIN],
        }
        default_role = 'Viewer'
        for r in roles:
            role = Role.query.filter_by(name=r).first()
            if role is None:
                role = Role(name=r)
            role.reset_permissions()
            for perm in roles[r]:
                role.add_permission(perm)
            role.default = (role.name == default_role)
            db.session.add(role)
        db.session.commit()

    def add_permission(self, perm):
        if not self.has_permission(perm):
            self.permissions += perm

    def remove_permission(self, perm):
        if self.has_permission(perm):
            self.permissions -= perm

    def reset_permissions(self):
        self.permissions = 0

    def has_permission(self, perm):
        return self.permissions & perm == perm


class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(64), unique=True, index=True)
    username = db.Column(db.String(64), unique=True, index=True)
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))
    password_hash = db.Column(db.String(128))
    confirmed = db.Column(db.Boolean, default=False)
    last_seen = db.Column(db.DateTime(), default=datetime.utcnow)
    last_login = db.Column(db.DateTime(), default=datetime.utcnow)
    last_password_update = db.Column(db.DateTime(), default=datetime.utcnow)
    created = db.Column(db.DateTime(), nullable=False, default=datetime.utcnow)
    updated = db.Column(db.DateTime(), nullable=False, default=datetime.utcnow)

    def __init__(self, **kwargs):
        super(User, self).__init__(**kwargs)
        if self.role is None:
            if self.email == current_app.config['FLASKY_ADMIN']:
                self.role = Role.query.filter_by(name='Administrator').first()
            if self.role is None:
                self.role = Role.query.filter_by(default=True).first()

    @property
    def password(self):
        raise AttributeError('password is not a readable attribute')

    @password.setter
    def password(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)

    def generate_confirmation_token(self, expiration=3600):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps({'confirm': self.id}).decode('utf-8')

    def confirm(self, token):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token.encode('utf-8'))
        except:
            return False
        if data.get('confirm') != self.id:
            return False
        self.confirmed = True
        db.session.add(self)
        return True

    def generate_reset_token(self, expiration=3600):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps({'reset': self.id}).decode('utf-8')

    @staticmethod
    def reset_password(token, new_password):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token.encode('utf-8'))
        except:
            return False
        user = User.query.get(data.get('reset'))
        if user is None:
            return False
        user.password = new_password
        db.session.add(user)
        return True

    def generate_email_change_token(self, new_email, expiration=3600):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps(
            {'change_email': self.id, 'new_email': new_email}).decode('utf-8')

    def change_email(self, token):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token.encode('utf-8'))
        except:
            return False
        if data.get('change_email') != self.id:
            return False
        new_email = data.get('new_email')
        if new_email is None:
            return False
        if self.query.filter_by(email=new_email).first() is not None:
            return False
        self.email = new_email
        # self.avatar_hash = self.gravatar_hash()
        db.session.add(self)
        return True

    def can(self, perm):
        return self.role is not None and self.role.has_permission(perm)

    def is_administrator(self):
        return self.can(Permission.ADMIN)

    def ping(self):
        self.last_seen = datetime.utcnow()
        db.session.add(self)


class AnonymousUser(AnonymousUserMixin):
    def can(self, permissions):
        return False

    def is_administrator(self):
        return False

login_manager.anonymous_user = AnonymousUser
