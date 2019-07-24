from . import redis_client, db, report_aggregate_inidicators


def get_indicators_from_rapidpro_results(results_json, indicator_conf={}, report_type=None):
    report_type_indicators = indicator_conf.get(report_type, [])
    # we shall have to sum up the aggregate inidicators to get a total
    aggregate_indicators = report_aggregate_inidicators.get(report_type, [])
    flow_inidicators = {}
    total_cases = 0

    for k, v in results_json.items():
        if k in report_type_indicators:
            if k == 'month':
                flow_inidicators[k] = results_json[k]['category']
            else:
                try:
                    flow_inidicators[k] = int(results_json[k]['value'])
                except:
                    flow_inidicators[k] = results_json[k]['value']
            # sum up aggregate indicators
            if k in aggregate_indicators and results_json[k]['value'].isdigit():
                total_cases += int(results_json[k]['value'])
    flow_inidicators['total_cases'] = total_cases
    return flow_inidicators
