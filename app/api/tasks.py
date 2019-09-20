from .. import db, celery, INDICATORS
from ..models import FlowData, PoliceStation, JusticeCourt, Location
from ..utils import get_indicators_from_rapidpro_results
from datetime import datetime
import calendar
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

MONTHS_DICT = dict((v, k) for k, v in enumerate(calendar.month_name))


@celery.task(name="tasks.save_flowdata")
def save_flowdata(request_args, request_json, districts, pstations, jcourts):
    msisdn = request_args.get('msisdn')
    report_type = request_args.get('report_type')
    station = request_args.get('station')
    district = request_args.get('district')

    flowdata = get_indicators_from_rapidpro_results(
        request_json['results'], INDICATORS, report_type)
    month = flowdata.pop('month')
    if month == 'December' or MONTHS_DICT[month] > datetime.now().month:
        year = datetime.now().year - 1
    else:
        year = datetime.now().year
    if report_type in ('ncjf'):
        year = datetime.now().year

    month_str = "{0}-{1:02}".format(year, MONTHS_DICT[month])

    # redis_client.districts set using @app.before_first_request
    ids = districts.get(district)
    if ids:
        logger.info(f'Going to save data for district: {district}, station: {station}')
        district_id = ids['id']
        region_id = ids['parent_id']
        if report_type in ('pvsu', 'diversion'):
            logger.info(f'Handling PVSU or Diversion Data for MSISDN: {msisdn}')
            police_station = pstations.get(station)
            value_record = FlowData.query.filter_by(
                year=year, month=month_str, station=police_station, report_type=report_type).first()

            if value_record:
                value_record.values = flowdata
                value_record.msisdn = msisdn
                value_record.updated = datetime.now()
            else:
                db.session.add(FlowData(
                    msisdn=msisdn, district=district_id, region=region_id, station=police_station,
                    report_type=report_type, month=month_str, year=year, values=flowdata))
            db.session.commit()

        elif report_type == 'ncjf':
            logger.info(f'Handling NCJF Data for MSISDN: {msisdn}')
            court = jcourts.get(station)
            value_record = FlowData.query.filter_by(
                year=year, month=month_str, court=court, report_type=report_type).first()

            if value_record:
                value_record.values = flowdata
                value_record.msisdn = msisdn
                value_record.updated = datetime.now()
            else:
                db.session.add(FlowData(
                    msisdn=msisdn, district=district_id, region=region_id, court=court,
                    report_type=report_type, month=month_str, year=year, values=flowdata))
            db.session.commit()
        logger.info(f'Done processing flow values')
    else:
        logger.info("district ids empty")
