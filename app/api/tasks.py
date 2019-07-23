from .. import db, celery, indicators
from ..models import FlowData
from ..utils import get_indicators_from_rapidpro_results
from datetime import datetime
import calendar

MONTHS_DICT = dict((v, k) for k, v in enumerate(calendar.month_name))


@celery.task(name="tasks.save_flowdata")
def save_flowdata(request_args, request_json, districts, pstations):
    msisdn = request_args.get('msisdn')
    report_type = request_args.get('report_type')
    station = request_args.get('station')
    district = request_args.get('district')

    flowdata = get_indicators_from_rapidpro_results(
        request_json['results'], indicators, report_type)
    month = flowdata.pop('month')
    if month == 'December' or MONTHS_DICT[month] > datetime.now().month:
        year = datetime.now().year - 1
    else:
        year = datetime.now().year
    month_str = "{0}-{1:02}".format(year, MONTHS_DICT[month])

    # redis_client.districts set using @app.before_first_request
    ids = districts.get(district)
    if ids:
        district_id = ids['id']
        region_id = ids['parent_id']
        police_station = pstations.get(station)

        db.session.add(FlowData(
            msisdn=msisdn, district=district_id, region=region_id, station=police_station,
            report_type=report_type, month=month_str, values=flowdata))
        db.session.commit()
