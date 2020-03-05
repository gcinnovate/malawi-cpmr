from .. import db, celery, INDICATORS
from ..models import (
    FlowData, TraditionalAuthority, ChildrensCorner, CommunityVictimSupportUnit, OneStopCenter)
from ..utils import get_indicators_from_rapidpro_results
from datetime import datetime
import calendar
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

MONTHS_DICT = dict((v, k) for k, v in enumerate(calendar.month_name))


@celery.task(name="tasks.save_flowdata")
def save_flowdata(
        request_args, request_json, districts, pstations=None, jcourts=None, cvsus=None):
    msisdn = request_args.get('msisdn')
    report_type = request_args.get('report_type')
    district = request_args.get('district').title()
    if not request_json:
        logger.info("EMPTY Post Data from RapidPro! [Report: {0}, MSISDN: {1}]".format(report_type, msisdn))
        return

    flowdata = get_indicators_from_rapidpro_results(
        request_json['results'], INDICATORS, report_type)
    month = flowdata.pop('month')
    if report_type in ('pvsu', 'diversion') and (month == 'December' or MONTHS_DICT[month] > datetime.now().month):
        year = datetime.now().year - 1
    elif report_type in ('ncjf', 'cc', 'cvsu', 'osc'):
        if 'year' in flowdata:
            year = flowdata.pop('year')
        else:
            if (month == 'December' or MONTHS_DICT[month] > datetime.now().month):
                year = datetime.now().year - 1
            else:
                year = datetime.now().year
    else:
        year = datetime.now().year

    month_str = "{0}-{1:02}".format(year, MONTHS_DICT[month])

    # redis_client.districts set using @app.before_first_request
    ids = districts.get(district)
    if ids:
        district_id = ids['id']
        region_id = ids['parent_id']
        if report_type in ('pvsu', 'diversion'):
            logger.info('Handling PVSU or Diversion Data for MSISDN: {0}'.format(msisdn))
            station = request_args.get('station').title()
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
            try:
                db.session.commit()
            except:
                db.session.rollback()
                logger.info('DB ERROR: [PVSU-Diversoin] [MSISDN: {0}, District: {1}, Court: {2}] [MONTH: {3}]'.format(
                    msisdn, district, station, month_str))

        elif report_type == 'ncjf':
            logger.info('Handling NCJF Data for MSISDN: {0}'.format(msisdn))
            station = request_args.get('station').title()
            # XXX
            court = jcourts.get(station, '')
            if court:
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
                try:
                    db.session.commit()
                except:
                    db.session.rollback()
                    logger.info('DB ERROR -Save Data: [NCJF] [MSISDN: {0}, District: {1}, Court: {2}] [MONTH: {3}]'.format(
                        msisdn, district, station, month_str))

        elif report_type == 'cvsu':
            ta = request_args.get('traditional_authority', '')
            cvsu = request_args.get('cvsu', '')
            logger.info('Handling CVSU Data for MSISDN: {0} TA:{1}, CVSU:{2}'.format(msisdn, ta, cvsu))
            ta_obj = TraditionalAuthority.query.filter_by(
                district_id=district_id).filter(TraditionalAuthority.name.ilike(ta)).first()
            if ta_obj:
                logger.info('TA Object Found for TA:{0}'.format(ta))
                cvsu_obj = CommunityVictimSupportUnit.query.filter_by(
                    name=cvsu.title(), district_id=district_id, ta_id=ta_obj.id).first()
                if not cvsu_obj:
                    cvsu_obj = CommunityVictimSupportUnit(
                        name=cvsu.title(), district_id=district_id, ta_id=ta_obj.id)
                    db.session.add(cvsu_obj)
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()
                        logger.info(
                            'DB ERROR - Add CVSU: [CVSU] [MSISDN: {0}, District: {1}, TA: {2}, CVSU: {3}] [MONTH: {4}]'.format(
                                msisdn, district, ta, cvsu, month_str))
            else:
                logger.info('TA Object NOT Found for TA:{0}'.format(ta))
                if 'Cvsu' in ta.title():
                    ta = ta.title().replace('Cvsu', 'CVSU')
                else:
                    ta = ta.title()
                db.session.add(TraditionalAuthority(district_id=district_id, name=ta))
                ta_obj = TraditionalAuthority.query.filter_by(district_id=district_id, name=ta).first()
                cvsu_obj = CommunityVictimSupportUnit(name=cvsu.title(), district_id=district_id, ta_id=ta_obj.id)
                db.session.add(cvsu_obj)
                try:
                    db.session.commit()
                except:
                    db.session.rollback()
                    logger.info(
                        'DB ERROR - Add CVSU: [CVSU] [MSISDN: {0}, District: {1}, TA: {2}, CVSU: {3}] [MONTH: {4}]'.format(
                            msisdn, district, ta, cvsu, month_str))

            value_record = FlowData.query.filter_by(
                year=year, month=month_str, cvsu=cvsu_obj.id, report_type=report_type).first()

            if value_record:
                logger.info('Values Record Object Found: MSISDN:{0}, Month: {1}, CVSU: {2}'.format(
                    msisdn, month_str, cvsu))
                value_record.values = flowdata
                value_record.msisdn = msisdn
                value_record.updated = datetime.now()
            else:
                logger.info('NO Values Record Object Found: MSISDN:{0}, Month: {1}, CVSU: {2}'.format(
                    msisdn, month_str, cvsu))
                db.session.add(FlowData(
                    msisdn=msisdn, district=district_id, region=region_id, cvsu=cvsu_obj.id,
                    report_type=report_type, month=month_str, year=year, values=flowdata))
            try:
                db.session.commit()
            except:
                db.session.rollback()
                logger.info(
                    'DB ERROR - Save Data: [CVSU] [MSISDN: {0}, District: {1}, TA: {2}, CVSU: {3}] [MONTH: {4}]'.format(
                        msisdn, district, ta, cvsu, month_str))

        elif report_type == 'cc':
            logger.info('Handling CC Data for MSISDN: {0}'.format(msisdn))
            ta = request_args.get('traditional_authority', '')
            childrens_corner = request_args.get('childrens_corner', '')
            ta_obj = TraditionalAuthority.query.filter_by(
                district_id=district_id).filter(TraditionalAuthority.name.ilike(ta)).first()
            if ta_obj:
                logger.info('TA Object Found for TA:{0}'.format(ta))
                cc_obj = ChildrensCorner.query.filter_by(
                    name=childrens_corner.title(), district_id=district_id, ta_id=ta_obj.id).first()
                if not cc_obj:
                    cc_obj = ChildrensCorner(
                        name=childrens_corner.title(), district_id=district_id, ta_id=ta_obj.id)
                    db.session.add(cc_obj)
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()
                        logger.info(
                            'DB ERROR - Add CC: [CC] [MSISDN: {0}, District: {1}, TA: {2}, CC: {3}] [MONTH: {4}]'.format(
                                msisdn, district, ta, childrens_corner, month_str))
            else:
                logger.info('TA Object NOT Found for TA:{0}'.format(ta))
                if 'Cvsu' in ta.title():
                    ta = ta.title().replace('Cvsu', 'CVSU')
                else:
                    ta = ta.title()
                db.session.add(TraditionalAuthority(district_id=district_id, name=ta))
                ta_obj = TraditionalAuthority.query.filter_by(district_id=district_id, name=ta).first()
                cc_obj = ChildrensCorner(name=childrens_corner, district_id=district_id, ta_id=ta_obj.id)
                db.session.add(cc_obj)
                try:
                    db.session.commit()
                except:
                    db.session.rollback()
                    logger.info(
                        'DB ERROR - Add CC: [CC] [MSISDN: {0}, District: {1}, TA: {2}, CC: {3}] [MONTH: {4}]'.format(
                            msisdn, district, ta, childrens_corner, month_str))

            value_record = FlowData.query.filter_by(
                year=year, month=month_str, children_corner=cc_obj.id,
                report_type=report_type).first()

            if value_record:
                logger.info('Values Record Object Found: MSISDN:{0}, Month: {1}, CC: {2}'.format(
                    msisdn, month_str, childrens_corner))
                value_record.values = flowdata
                value_record.msisdn = msisdn
                value_record.updated = datetime.now()
            else:
                logger.info('NO Values Record Object Found: MSISDN:{0}, Month: {1}, CC: {2}'.format(
                    msisdn, month_str, childrens_corner))
                db.session.add(FlowData(
                    msisdn=msisdn, district=district_id, region=region_id, children_corner=cc_obj.id,
                    report_type=report_type, month=month_str, year=year, values=flowdata))
            try:
                db.session.commit()
            except:
                db.session.rollback()
                logger.info(
                    'DB ERROR - Save Data: [CC] [MSISDN: {0}, District: {1}, TA: {2}, CC: {3}] [MONTH: {4}]'.format(
                        msisdn, district, ta, childrens_corner, month_str))

        # Handle One Stop Center data
        elif report_type == 'osc':
            one_stop_center = request_args.get('one_stop_center').title()
            logger.info('Handling OSC Data [MSISDN: {0}, District: {1}, OSC: {2}]'.format(msisdn, district, one_stop_center))
            if one_stop_center:
                osc_obj = OneStopCenter.query.filter_by(
                    district_id=district_id).filter(OneStopCenter.name.ilike(one_stop_center)).first()
                if osc_obj:
                    logger.info('OSC Object Found for OSC: {0}'.format(one_stop_center))
                else:
                    db.session.add(OneStopCenter(district_id=district_id, name=one_stop_center.title()))
                    osc_obj = OneStopCenter.query.filter_by(district_id=district_id, name=one_stop_center).first()
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()
                        logger.info(
                            'DB ERROR - Add OSC: [CC] [MSISDN: {0}, District: {1}, OSC: {2}] [MONTH: {4}]'.format(
                                msisdn, district, one_stop_center, month_str))

                # save the data
                value_record = FlowData.query.filter_by(
                    year=year, month=month_str, one_stop_center=osc_obj.id,
                    report_type=report_type).first()
                if value_record:
                    logger.info('Values Record Object Found: MSISDN:{0}, Month: {1}, OSC: {2}'.format(
                        msisdn, month_str, one_stop_center))
                    value_record.values = flowdata
                    value_record.msisdn = msisdn
                    value_record.updated = datetime.now()
                else:
                    logger.info('NO Values Record Object Found: MSISDN:{0}, Month: {1}, OSC: {2}'.format(
                        msisdn, month_str, one_stop_center))
                    db.session.add(FlowData(
                        msisdn=msisdn, district=district_id, region=region_id, one_stop_center=osc_obj.id,
                        report_type=report_type, month=month_str, year=year, values=flowdata))
                try:
                    db.session.commit()
                except:
                    db.session.rollback()
                    logger.info(
                        'DB ERROR - Save Data: [OSC] [MSISDN: {0}, District: {1}, OSC: {2}] [MONTH: {3}]'.format(
                            msisdn, district, one_stop_center, month_str))
            else:
                logger.info(
                    "OSC Data: One Stop Center missing [MSISDN: {0}, District: {1}, OSC: {2}]".format(
                        msisdn, district, one_stop_center))

        logger.info('Done processing flow values')
    else:
        logger.info("district ids empty for MSISDN: {0}, District: {1}".format(msisdn, district))
