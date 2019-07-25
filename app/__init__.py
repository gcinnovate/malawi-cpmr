from flask import Flask, request
from flask_mail import Mail
from flask_moment import Moment
from flask_sqlalchemy import SQLAlchemy
from celery import Celery
from config import config, Config, INDICATORS, REPORT_AGGREGATE_INIDICATORS
from flask_login import LoginManager
from flask_redis import FlaskRedis

mail = Mail()
moment = Moment()
db = SQLAlchemy()
redis_client = FlaskRedis()
celery = Celery(__name__, broker=Config.CELERY_BROKER_URL)

login_manager = LoginManager()


def create_app(config_name='default'):
    # create and configure the app
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    login_manager.init_app(app)
    mail.init_app(app)
    moment.init_app(app)
    db.init_app(app)
    redis_client.init_app(app)
    celery.conf.update(app.config)

    from .api import api as api_blueprint
    app.register_blueprint(api_blueprint, url_prefix='/api/v1')

    # a simple page that says hello
    @app.route('/hello', methods=['GET', 'POST'])
    def hello():
        if request.method == 'POST':
            print(request.args)
            print("JSON>>>>>>>>>>>>")
            print(request.json)
        if request.method == 'GET':
            print(__name__, "><<<<<<")
            print(request.args)
        return 'Hello, World!'

    return app
