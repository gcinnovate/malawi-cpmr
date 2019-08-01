import os
import random

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'some random string'
    MAIL_SERVER = os.environ.get('MAIL_SERVER', 'smtp.googlemail.com')
    MAIL_PORT = int(os.environ.get('MAIL_PORT', '587'))
    MAIL_USE_TLS = os.environ.get('MAIL_USE_TLS', 'true').lower() in \
        ['true', 'on', '1']
    MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
    MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
    FLASKY_MAIL_SUBJECT_PREFIX = '[CMPR]'
    FLASKY_MAIL_SENDER = 'CPMR Admin <cpmr@example.com>'
    FLASKY_ADMIN = os.environ.get('CPMR_ADMIN')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REDIS_URL = CELERY_RESULT_BACKEND = os.environ.get('REDIS_URL')
    CELERY_BROKER_URL = os.environ.get('REDIS_URL')
    CELERY_RESULT_BACKEND = os.environ.get('REDIS_URL')

    @staticmethod
    def init_app(app):
        pass


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = os.environ.get('DEV_DATABASE_URL') or \
        'sqlite:///' + os.path.join(basedir, 'data-dev.sqlite')


class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = os.environ.get('TEST_DATABASE_URL') or \
        'sqlite://'


class ProductionConfig(Config):
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'sqlite:///' + os.path.join(basedir, 'data.sqlite')

config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}

INDICATORS = {
    'pvsu': [
        'month', 'physicalviolence', 'suicide', 'defilement', 'rape', 'indecentassault',
        'humantrafficking', 'kidnapping', 'sexualoffences', 'maritalconflict', 'childneglect',
        'economicabuse',
        'boys_physicalviolence', 'girls_physicalviolence', 'men_physicalviolence', 'women_physicalviolence',
        'boys_suicide', 'girls_suicide', 'men_suicide', 'women_suicide',
        'girls_defilement',
        'girls_rape', 'women_rape',
        'boys_indecentassault', 'girls_indecentassault', 'men_indecentassault', 'women_indecentassault',
        'boys_humantrafficking', 'girls_humantrafficking', 'men_humantrafficking', 'women_humantrafficking',
        'boys_kidnapping', 'girls_kidnapping', 'men_kidnapping', 'women_kidnapping',
        'boys_sexualoffences', 'girls_sexualoffences', 'men_sexualoffences', 'women_sexualoffences',
        'boys_maritalconflict', 'girls_maritalconflict', 'men_maritalconflict', 'women_maritalconflict',
        'boys_childneglect', 'girls_childneglect',
        'boys_economicabuse', 'girls_economicabuse', 'men_economicabuse', 'women_economicabuse',
    ],
    'diversion': [
        'month',
        'arrested', 'divertedatpolice', 'takentocourt', 'bailed', 'releasedfreely', 'releasedin24hrs',
        'boys_arrested', 'girls_arrested',
        'boys_divertedatpolice', 'girls_divertedatpolice',
        'boys_takentocourt', 'girls_takentocourt',
        'boys_bailed', 'girls_bailed',
        'boys_releasedfreely', 'girls_releasedfreely',
        'boys_releasedin24hrs', 'girls_releasedin24hrs',
    ]
}

REPORT_AGGREGATE_INIDICATORS = {
    'pvsu': [
        'physicalviolence', 'suicide', 'defilement', 'rape', 'indecentassault', 'humantrafficking',
        'kidnapping', 'sexualoffences', 'maritalconflict', 'childneglect', 'economicabuse'
    ],
    'diversion': [
        'arrested', 'divertedatpolice', 'takentocourt', 'bailed',
        'releasedfreely', 'releasedin24hrs'
    ]
}

# The following are used for generating dummy data

INDICATOR_CATEGORY_MAPPING = {
    'pvsu': {
        'physicalviolence': ['boys', 'girls', 'men', 'women'],
        'suicide': ['boys', 'girls', 'men', 'women'],
        'defilement': ['girls'],
        'rape': ['girls', 'women'],
        'indecentassault': ['boys', 'girls', 'men', 'women'],
        'humantrafficking': ['boys', 'girls', 'men', 'women'],
        'kidnapping': ['boys', 'girls', 'men', 'women'],
        'sexualoffences': ['boys', 'girls', 'men', 'women'],
        'maritalconflict': ['boys', 'girls', 'men', 'women'],
        'childneglect': ['boys', 'girls', 'men', 'women'],
        'economicabuse': ['boys', 'girls', 'men', 'women']
    },
    'diversion': {
        'arrested': ['boys', 'girls'],
        'divertedatpolice': ['boys', 'girls'],
        'takentocourt': ['boys', 'girls'],
        'bailed': ['boys', 'girls'],
        'releasedfreely': ['boys', 'girls'],
        'releasedin24hrs': ['boys', 'girls'],
    }
}

# the guide random generation
INDICATOR_THRESHOLD = {
    'physicalviolence': random.randint(0, 100),
    'suicide': random.randint(0, 20),
    'defilement': random.randint(0, 40),
    'rape': random.randint(0, 20),
    'indecentassault': random.randint(0, 100),
    'humantrafficking': random.randint(0, 30),
    'kidnapping': random.randint(0, 60),
    'sexualoffences': random.randint(0, 80),
    'maritalconflict': random.randint(0, 60),
    'childneglect': random.randint(0, 40),
    'economicabuse': random.randint(0, 120),
    'arrested': 1000,
    'divertedatpolice': 200,
    'bailed': 100,
    'takentocourt': 80,
    'releasedfreely': 40,
    'releasedin24hrs': 30
}
