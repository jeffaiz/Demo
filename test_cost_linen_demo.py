def calculate_thresholds_products(event, flag_value, flag_percent, value, percent, currency_symbol):
    """
        * Function to aggregate thresholds for water,energy and chemicals
    """
    try:

        if flag_value == True:

            if event['groupby'] == 'week':
                value = (7 * value) / len(event['sites'])

            if event['groupby'] == 'hour':
                value = value / (24 * len(event['sites']))

            if event['groupby'] == 'day':
                value = value / len(event['sites'])

            if event['groupby'] == 'month':
                value = (event['days'] * value) / len(event['sites'])


        else:
            value = None
        if flag_percent == True:
            if event['groupby'] == 'week':
                percent = (7 * percent) / len(event['sites'])

            if event['groupby'] == 'hour':
                percent = percent / (24 * len(event['sites']))
            if event['groupby'] == 'day':
                percent = percent / len(event['sites'])

            if event['groupby'] == 'month':
                percent = (event['days'] * percent) / len(event['sites'])
        else:
            percent = None
        print(value)

        print("conversion method")
        user_currency = event['currency']
        if user_currency == currency_symbol:
            return {'threshold': value, 'threshold_per_linen': percent}
            else:
            converted_value = convert_total_cost(value, user_currency)
            print(converted_value)
            return {'threshold': converted_value, 'threshold_per_linen': percent}
    # logger.info({'threshold': converted_value, 'threshold_per_linen': percent})
    # logger.info("After threshold")
    # return {'threshold': converted_value, 'threshold_per_linen': percent}

    except Exception as e:
        logger.error(e)
        logger.error("Failed while calculating thresholds")
