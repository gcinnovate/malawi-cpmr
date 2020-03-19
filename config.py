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
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,
        "pool_recycle": 300,
    }

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


# Flows and their associated flow variables. keys represent flows
INDICATORS = {
    'pvsu': [
        'month', 'physicalviolence', 'suicide', 'defilement', 'rape', 'indecentassault',
        'humantrafficking', 'kidnapping', 'sexualoffences', 'maritalconflict', 'childneglect',
        'economicabuse', 'breachofpeace',
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
        'boys_breachofpeace', 'girls_breachofpeace', 'men_breachofpeace', 'women_breachofpeace'
    ],
    'diversion': [
        'month',
        'arrested', 'divertedatpolice', 'takentocourt', 'bailed', 'releasedfreely', 'releasedin48hrs',
        'boys_arrested', 'girls_arrested',
        'boys_divertedatpolice', 'girls_divertedatpolice',
        'boys_takentocourt', 'girls_takentocourt',
        'boys_bailed', 'girls_bailed',
        'boys_releasedfreely', 'girls_releasedfreely',
        'boys_releasedin48hrs', 'girls_releasedin48hrs',
    ],
    'ncjf': [
        'month', 'year',
        'fromprevmonth_cvc', 'newlyregistered_cvc', 'newlyregconcluded_cvc', 'concluded_cvc',
        'defilement_cvccategory', 'childmarriage_cvccategory', 'childtrafficking_cvccategory',
        'sexualviolence_cvccategory', 'concluded', 'total_childcases',
        'cvc', 'imprisoned_perpetrators', 'acquitted_perpetrators', 'fined_perpetrators',
        'perpetrators', 'caseswithdrawn', 'referredchildsurvivors', 'fromprevmonth_cbc',
        'newlyregistered_cbc', 'newlyregconcluded_cbc', 'concluded_cbc', 'cbc',
        'childmaintenance_cbctype', 'childcustody_cbctype', 'childfosterage_cbctype', 'childadoption_cbctype',
        'childguardianship_cbctype', 'childaccess_cbctype', 'childparentage_cbctype', 'estatedistribution_cbctype',
        'cbctype', 'fromprevmonth_inconflict', 'newlyregistered_inconflict', 'newlyregconcluded_inconflict',
        'concluded_inconflict', 'inconflict', 'preliminaryinquiry_diverted', 'aftertrial_diverted', 'diverted', 'bailed',
        'total_custodialorder', 'reformatories_custodialorder', 'prisons_custodialorder', 'total_remanded',
        'safetyhomes_remanded', 'reformatorycentres_remanded', 'policecells_remanded', 'specialreferrals',
    ],
    'cvsu': [
        'month', 'year', 'physicalviolence', 'defilement', 'sexualviolence', 'childneglect', 'childmarriage',
        'emotionalabuse', 'economicexploitation', 'economicabuse', 'maritalconflict', 'humantrafficking',
        'boys_physicalviolence', 'girls_physicalviolence', 'men_physicalviolence', 'women_physicalviolence',
        'girls_defilement',
        'boys_sexualviolence', 'girls_sexualviolence', 'men_sexualviolence', 'women_sexualviolence',
        'boys_childneglect', 'girls_childneglect',
        'boys_childmarriage', 'girls_childmarriage',
        'boys_emotionalabuse', 'girls_emotionalabuse', 'men_emotionalabuse', 'women_emotionalabuse',
        'boys_economicexploitation', 'girls_economicexploitation', 'men_economicexploitation',
        'women_economicexploitation',
        'boys_humantrafficking', 'girls_humantrafficking', 'men_humantrafficking', 'women_humantrafficking',
        'boys_economicabuse', 'girls_economicabuse', 'men_economicabuse', 'women_economicabuse',
        'boys_maritalconflict', 'girls_maritalconflict', 'men_maritalconflict', 'women_maritalconflict',

    ],
    'cc': [
        'month', 'year', 'attendance', 'violence', 'referred',
        'boys_attendance', 'girls_attendance',
        'boys_violence', 'girls_violence',
        'boys_referred', 'girls_referred',
        'men_trainedfacilitators', 'women_trainedfacilitators',
        'men_nontrainedfacilitators', 'women_nontrainedfacilitators',
        'trainedfacilitators', 'nontrainedfacilitators'
    ],
    'osc': [
        'month', 'year', 'boys_physicalviolence', 'girls_physicalviolence', 'men_physicalviolence',
        'women_physicalviolence', 'physicalviolence',
        'boys_sexualviolence', 'girls_sexualviolence', 'men_sexualviolence',
        'women_sexualviolence', 'sexualviolence', 'boys_psychosocialsupport',
        'girls_psychosocialsupport', 'men_psychosocialsupport', 'women_psychosocialsupport', 'psychosocialsupport',
        'male_perpetrators_physicalviolence', 'female_perpetrators_physicalviolence',
        'male_perpetrators_sexualviolence', 'female_perpetrators_sexualviolence',
        'parents_relation_physicalviolence', 'relatives_relation_physicalviolence',
        'boygirlfriend_relation_physicalviolence', 'husbandwife_relation_physicalviolence',
        'victimknows_relation_physicalviolence', 'strangers_relation_physicalviolence',
        'parents_relation_sexualviolence', 'relatives_relation_sexualviolence',
        'boygirlfriend_relation_sexualviolence', 'husbandwife_relation_sexualviolence',
        'victimknows_relation_sexualviolence', 'strangers_relation_sexualviolence',
        'referredfrom_self', 'referredfrom_socailwelfare', 'referredfrom_police',
        'referredfrom_hospital', 'referredfrom_others', 'male_feltsafe', 'female_feltsafe',
        'male_feltunsafe', 'female_feltunsafe'
    ]
}

REPORT_AGGREGATE_INIDICATORS = {
    'pvsu': [
        'physicalviolence', 'suicide', 'defilement', 'rape', 'indecentassault', 'humantrafficking',
        'kidnapping', 'sexualoffences', 'maritalconflict', 'childneglect', 'economicabuse',
        'breachofpeace'
    ],
    'diversion': [
        'arrested', 'divertedatpolice', 'takentocourt', 'bailed',
        'releasedfreely', 'releasedin48hrs'
    ],
    'ncjf': [
        'perpetrators', 'cbc', 'cvc', 'inconflict'
    ],
    'cvsu': [
        'physicalviolence', 'defilement', 'childneglect', 'childmarriage', 'economicabuse',
        'economicexploitation', 'maritalconflict', 'sexualviolence', 'humantrafficking',
        'emotionalabuse'
    ],
    'cc': [
        'attendance', 'violence', 'referred'
    ],
    'osc': [
        'physicalviolence', 'sexualviolence', 'psychosocialsupport', 'feltsafe', 'feltunsafe',
        'referredfrom'
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
        'economicabuse': ['boys', 'girls', 'men', 'women'],
        'breachofpeace': ['boys', 'girls', 'men', 'women'],
    },
    'diversion': {
        'arrested': ['boys', 'girls'],
        'divertedatpolice': ['boys', 'girls'],
        'takentocourt': ['boys', 'girls'],
        'bailed': ['boys', 'girls'],
        'releasedfreely': ['boys', 'girls'],
        'releasedin48hrs': ['boys', 'girls'],
    },
    'ncjf': {
        'cvc': ['fromprevmonth', 'newlyregistered', 'newlyregconcluded', 'concluded'],
        'cbc': ['fromprevmonth', 'newlyregistered', 'newlyregconcluded', 'concluded'],
        'cbctype': [
            'childmaintenance', 'childcustody', 'childfosterage', 'childadoption',
            'childparentage', 'childguardianship', 'childaccess', 'estatedistribution'],
        'cvccategory': ['defilement', 'childmarriage', 'childtrafficking', 'sexualviolence'],
        'inconflict': ['fromprevmonth', 'newlyregistered', 'newlyregconcluded', 'concluded'],
        'custodialorder': ['total', 'reformatories', 'prisons'],
        'remanded': ['total', 'safetyhomes', 'reformatorycentres', 'policecells'],
        'diverted': ['preliminaryinquiry', 'aftertrial'],
        'perpetrators': ['imprisoned', 'acquited', 'fined'],
        'bailed': [],
        'specialreferrals': [],
        'caseswithdrawn': [],
        'referredchildsurvivors': [],
        'concluded': [],
        'total_childcases': []

    },
    'cvsu': {
        'physicalviolence': ['boys', 'girls', 'men', 'women'],
        'defilement': ['girls'],
        'sexualviolence': ['boys', 'girls', 'men', 'women'],
        'humantrafficking': ['boys', 'girls', 'men', 'women'],
        'childneglect': ['boys', 'girls'],
        'childmarriage': ['boys', 'girls'],
        'economicexploitation': ['boys', 'girls', 'men', 'women'],
        'economicabuse': ['boys', 'girls', 'men', 'women'],
        'emotionalabuse': ['boys', 'girls', 'men', 'women'],
        'maritalconflict': ['boys', 'girls', 'men', 'women'],
    },
    'cc': {
        'attendance': ['boys', 'girls'],
        'violence': ['boys', 'girls'],
        'referred': ['boys', 'girls'],
        'trainedfacilitators': ['men', 'women'],
        'nontrainedfacilitators': ['men', 'women'],

    },
    'osc': {
        'physicalviolence': ['boys', 'girls', 'men', 'women'],
        'sexualviolence': ['boys', 'girls', 'men', 'women'],
        'psychosocialsupport': ['boys', 'girls', 'men', 'women'],
        'referredfrom': ['self', 'hospital', 'socialwelfare', 'police', 'others'],
        'perpetrators_physicalvionce': ['male', 'female'],
        'perpetrators_sexualviolence': ['male', 'female'],
        'relation_physicalviolence': ['parents', 'relatives', 'boygirlfriend', 'husbandwife', 'victimknows', 'strangers'],
        'relation_sexualviolence': ['parents', 'relatives', 'boygirlfriend', 'husbandwife', 'victimknows', 'strangers'],
        'feltsafe': ['male', 'female'],
        'feltunsafe': ['male', 'female'],
    }
}

# the guide random generation for the flow variables
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
    'breachofpeace': random.randint(0, 140),
    'arrested': 300,
    'divertedatpolice': 200,
    'bailed': 100,
    'takentocourt': 80,
    'releasedfreely': 40,
    'releasedin48hrs': 200,
    'specialreferrals': 40,
    'caseswithdrawn': 15,
    'referredchildsurvivors': 25,
    'cvc': 200,
    'cbctype': 300,
    'cbc': 150,
    'cvccategory': 80,
    'inconflict': 75,
    'custodialorder': 55,
    'remanded': 25,
    'diverted': 15,
    'perpetrators': 60,
    'concluded': 200,
    'total_childcases': random.randint(500, 1000),
    'sexualviolence': 40,
    'emotionalabuse': 70,
    'economicexploitation': 55,
    'childmarriage': 30,
    'referred': 54,
    'violence': 70,
    'attendance': 150,
    'trainedfacilitators': 25,
    'nontrainedfacilitators': 10,
    'sexualviolence': 40,
    'psychosocialsupport': 30,
    'feltsafe': 20,
    'feltunsafe': 10,
    'relation_sexualviolence': 35,
    'relation_physicalviolence': 50,
    'referredfrom': 45,
    'perpetrators_physicalvionce': 27,
    'perpetrators_sexualviolence': 15
}
# a mapping of flow variables to their readable description
INDICATOR_NAME_MAPPING = {
    'physicalviolence': 'Physical Violence',
    'suicide': 'Suicide',
    'defilement': 'Defilement',
    'rape': 'Rape',
    'indecentassault': 'Indecent assault',
    'humantrafficking': 'Human trafficking',
    'kidnapping': 'Abduction',
    'sexualoffences': 'Other sexual violence',
    'sexualviolence': 'Other sexual violence',
    'maritalconflict': 'Marital and interpersonal conflict',
    'childneglect': 'Child neglect',
    'economicabuse': 'Economic abuse',
    'breachofpeace': 'Conduct likely to cause breach of peace',
    'childtrafficking': 'Child Trafficking',
    'childmarriage': 'Child Marriage',
    'economicexploitation': 'Economic exploitation',
    'emotionalabuse': 'Emotional abuse',
    'trainedfacilitators': 'Trained',
    'nontrainedfacilitators': 'Not Trained',
    # demorgraphics
    'boys_total': 'Boys',
    'girls_total': 'Girls',
    'men_total': 'Men',
    'women_total': 'Women',
    'referredfrom_self': 'Self, including brought from family',
    'referredfrom_socialwelfare': 'Social Welfare',
    'referredfrom_hospital': 'Hospital',
    'referredfrom_police': 'Police',
    'referredfrom_others': 'Others',

}

# Flow names that use a month generated from reporting date
AUTO_MONTH_FLOWS = []
INDICATORS_TO_SWAP_KEYVALS = ['referredfrom']
try:
    from local_config import *
except ImportError:
    pass
