from . import redis_client, db


def get_indicators_from_rapidpro_results(results_json, indicator_conf={}, report_type=None):
    report_type_indicators = indicator_conf.get(report_type, [])
    flow_inidicators = {}

    for k, v in results_json.items():
        if k in report_type_indicators:
            if k == 'month':
                flow_inidicators[k] = results_json[k]['category']
            else:
                try:
                    flow_inidicators[k] = int(results_json[k]['value'])
                except:
                    flow_inidicators[k] = results_json[k]['value']
    return flow_inidicators
