import os

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

indicators = {
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
        'boys_kidnapping', 'girls_kidnapping', 'kidnapping', 'men_kidnapping',
        'boys_sexualoffences', 'girls_sexualoffences', 'men_sexualoffences', 'women_sexualoffences',
        'boys_maritalconflict', 'girls_maritalconflict', 'men_maritalconflict', 'women_maritalconflict',
        'boys_childneglect', 'girls_childneglect',
        'boys_economicabuse', 'girls_economicabuse', 'men_economicabuse', 'women_economicabuse',
    ],
    'cvsu': [

    ]
}
