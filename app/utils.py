from . import redis_client, db, REPORT_AGGREGATE_INIDICATORS, AUTO_MONTH_FLOWS


def get_indicators_from_rapidpro_results(results_json, indicator_conf={}, report_type=None):
    report_type_indicators = indicator_conf.get(report_type, [])
    # we shall have to sum up the aggregate inidicators to get a total
    aggregate_indicators = REPORT_AGGREGATE_INIDICATORS.get(report_type, [])
    flow_inidicators = {}
    total_cases = 0
    boys_total = 0
    girls_total = 0
    men_total = 0
    women_total = 0

    for k, v in results_json.items():
        if k in report_type_indicators:
            if k == 'month':
                if report_type in AUTO_MONTH_FLOWS:
                    flow_inidicators[k] = results_json[k]['value']
                else:
                    flow_inidicators[k] = results_json[k]['category']
            else:
                try:
                    flow_inidicators[k] = int(results_json[k]['value'])
                except:
                    flow_inidicators[k] = results_json[k]['value']
            # sum up aggregate indicators
            if k in aggregate_indicators and results_json[k]['value'].isdigit():
                total_cases += int(results_json[k]['value'])
            if k.startswith('boys_'):
                boys_total += int(results_json[k]['value'])
            elif k.startswith('girls_'):
                girls_total += int(results_json[k]['value'])
            elif k.startswith('men_'):
                men_total += int(results_json[k]['value'])
            elif k.startswith('women_'):
                women_total += int(results_json[k]['value'])

    if report_type in ('pvsu', 'diversion', 'cvsu'):
        flow_inidicators['total_cases'] = total_cases

        flow_inidicators['boys_total'] = boys_total
        flow_inidicators['girls_total'] = girls_total
        flow_inidicators['men_total'] = men_total
        flow_inidicators['women_total'] = women_total
    return flow_inidicators
