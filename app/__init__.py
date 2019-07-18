import os

from flask import Flask
from flask_mail import Mail
from flask_sqlalchemy import SQLAlchemy
from config import config
from flask_login import LoginManager

mail = Mail()
db = SQLAlchemy()

login_manager = LoginManager()


def create_app(config_name='default'):
    # create and configure the app
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    # login_manager.init_app(app)
    db.init_app(app)

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        return 'Hello, World!'

    return app
