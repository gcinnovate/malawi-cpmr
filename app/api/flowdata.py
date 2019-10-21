from flask import jsonify, request
# from .. import db, INDICATORS
# from ..models import FlowData, Permission
from . import api
# from .decorators import permission_required
# from .errors import forbidden
from .. import redis_client
from .tasks import save_flowdata


@api.route('/flowdata')
def get_flowdata():
    return "Text flow data"


@api.route('/flowdata/', methods=['POST'])
def flowdata_webhook():

    # redis_client.districts set using @app.before_first_request
    districts = redis_client.districts
    report_type = request.args.get('report_type', '')

    if report_type in ('pvsu', 'diversion'):
        police_stations = redis_client.police_stations
        save_flowdata.delay(
            request.args, request.json, districts, pstations=police_stations)

    elif report_type == 'ncjf':
        justice_courts = redis_client.justice_courts
        save_flowdata.delay(
            request.args, request.json, districts, jcourts=justice_courts)

    elif report_type == 'cvsu':
        # tas = redis_client.traditional_auths
        save_flowdata.delay(
            request.args, request.json, districts)

    elif report_type == 'cc':
        # tas = redis_client.traditional_auths
        save_flowdata.delay(
            request.args, request.json, districts)

    return jsonify({'message': 'success'})
