import boto3
import logging
from boto3.dynamodb.conditions import Key, Attr
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def calculate_thresholds_products(event, flag_value, flag_percent, value, percent):
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
                value = value/len(event['sites'])

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
        """ *Comapere wheteher Input threshold is coming from COST, to convert it into user associated currency """
        
        threshold_names = ['water_costs', 'energy_costs', 'chemical_costs']
        if event['threshold'] in threshold_names:
            if event['currency']=='USD':
                return {'threshold': value, 'threshold_per_linen': percent}
            else:
                converted_value=convert_total_cost(value,event['currency'])
                converted_percent=convert_total_cost(percent,event['currency'])
                logger.info({'threshold': converted_value, 'threshold_per_linen': converted_percent})
                logger.info("After threshold")
                return {'threshold': converted_value, 'threshold_per_linen': converted_percent}   
        else:
            logger.info({'threshold': value, 'threshold_per_linen': percent})
            logger.info("After threshold")
            return {'threshold': value, 'threshold_per_linen': percent}
        
    except Exception as e:
        logger.error(e)
        logger.error("Failed while calculating thresholds")
      
def convert_total_cost(value,user_currency):
    """
        * Function to convert total threshold value for COST into user's associated currency
    """  
    print("inside convert_total")
    try:
        c = CurrencyRates()
        result=c.convert('USD',user_currency,value)
        return round(result, 2)
    except Exception as e:
        logger.error(e)
        logger.error("Failed while converting currency")


def calculate_thresholds_linen(event, flag_value, flag_percent, value, percent):
    """
        * Function to aggregate thresholds for rewash and aborted wash
    """

    try:

        if flag_value == True:

            if event['groupby'] == 'week':
                value = (7 * value) / len(event['sites'])

            if event['groupby'] == 'hour':
                value = value / (24 * len(event['sites']))
            if event['groupby'] == 'day':
                value = value/len(event['sites'])

            if event['groupby'] == 'month':
                value = (event['days'] * value) / len(event['sites'])
        else:

            value = None
        if flag_percent == True:

            if event['groupby'] == 'week':

                percent=percent/len(event['sites'])

            if event['groupby'] == 'hour':
                percent=percent / len(event['sites'])
            if event['groupby'] == 'day':
                percent = percent / len(event['sites'])

            if event['groupby'] == 'month':
                percent=percent / len(event['sites'])
        else:

            percent=None
            
        threshold_names = ['water_costs', 'energy_costs', 'chemical_costs']
        if event['threshold'] in threshold_names:
            if event['currency']=='USD':
                return {'threshold': value, 'threshold_per_linen': percent}
            else:
                converted_value=convert_total_cost(value,event['currency'])
                converted_percent=convert_total_cost(percent,event['currency'])
                logger.info({'threshold': converted_value, 'threshold_per_linen': converted_percent})
                logger.info("After threshold")
                return {'threshold': converted_value, 'threshold_per_linen': converted_percent}   
        else:
            logger.info({'threshold': value, 'threshold_per_linen': percent})
            logger.info("After threshold")
            return {'threshold': value, 'threshold_per_linen': percent}
        
    except Exception as e:
        logger.error(e)
        logger.error("Failed while calculating thresholds")

def convert_total_cost(value,user_currency):
    print("inside convert_currency")
    #print(value)
    try:
        c = CurrencyRates()
        result=c.convert('USD',user_currency,value)
        return round(result, 2)
    except Exception as e:
        logger.error(e)
        logger.error("Failed while converting currency")

def get_thresholds(event, th_type, sites):
    try:

        print("function called")
        print(sites)
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('Intellilinen_Thresholds')
        flag_value = True
        flag_percent = True
        value = 0
        percent = 0

        for site in sites:

            response = table.query(
                IndexName='Site_ID-index',
                KeyConditionExpression=Key('Site_ID').eq(site),
                FilterExpression=Attr('active').eq(
                    True) & Attr('End_time').not_exists()
            )
            site_threshold = response['Items'][0]['Thresholds'] if len(
                response['Items']) > 0 else []

            if th_type == 'production':
                threshold = 0
                threshold_percent = 0

                for i in site_threshold:

                    if 'production' in i:
                        prod_threshold = i['production']['loads'] if 'loads' in i['production'] else None
                        if prod_threshold != None:

                            if prod_threshold['active'] == True and 'threshold' in prod_threshold:
                                threshold = prod_threshold['threshold']
                if threshold == 0:

                    return {'threshold': None}
                else:
                    value += int(threshold)
                logger.info(value)
                logger.info("After value")
                
            if th_type == 'rewash':
                threshold = 0
                threshold_percent = 0

                for i in site_threshold:

                    if 'quality' in i:
                        rewash_threshold = i['quality']['rewashes'] if 'rewashes' in i['quality'] else None
                        if rewash_threshold != None:

                            if rewash_threshold['active'] == True and 'threshold' in rewash_threshold:
                                threshold = rewash_threshold['threshold']
                            if rewash_threshold['active'] == True and 'threshold_percent' in rewash_threshold:
                                threshold_percent = rewash_threshold['threshold_percent']
                print(threshold)
                print(threshold_percent)
                print("after first")
                if threshold == 0:
                    flag_value = False
                else:
                    value += threshold
                if threshold_percent == 0:
                    flag_percent = False
                else:
                    percent += threshold_percent
            if th_type == 'aborted':
                threshold = 0
                threshold_percent = 0

                for i in site_threshold:

                    if 'quality' in i:
                        aborted_threshold = i['quality']['aborted'] if 'aborted' in i['quality'] else None
                        if aborted_threshold != None:

                            if aborted_threshold['active'] == True and 'threshold' in aborted_threshold:
                                threshold = aborted_threshold['threshold']
                            if aborted_threshold['active'] == True and 'threshold_percent' in aborted_threshold:
                                threshold_percent = aborted_threshold['threshold_percent']
                print(threshold)
                print(threshold_percent)
                print("after first")
                if threshold == 0:
                    flag_value = False
                else:
                    value += threshold
                if threshold_percent == 0:
                    flag_percent = False
                else:
                    percent += threshold_percent
            if th_type == 'energy':
                threshold = 0
                threshold_per_linen = 0

                for i in site_threshold:

                    if 'consumption' in i:
                        energy_threshold = i['consumption']['energy_consumption_washer'] if 'energy_consumption_washer' in i['consumption'] else None
                        if energy_threshold != None:

                            if energy_threshold['active'] == True and 'threshold' in energy_threshold:
                                threshold = energy_threshold['threshold']
                            if energy_threshold['active'] == True and 'threshold_per_linen' in energy_threshold:
                                threshold_per_linen = energy_threshold['threshold_per_linen']
                print(threshold)
                print(threshold_per_linen)
                print("after first")
                if threshold == 0:
                    flag_value = False
                else:
                    value += threshold
                if threshold_per_linen == 0:
                    flag_percent = False
                else:
                    percent += threshold_per_linen
            if th_type == 'chemical':
                threshold = 0
                threshold_per_linen = 0

                for i in site_threshold:

                    if 'consumption' in i:
                        chemical_threshold = i['consumption']['chemical_consumption'] if 'chemical_consumption' in i['consumption'] else None
                        if chemical_threshold != None:

                            if chemical_threshold['active'] == True and 'threshold' in chemical_threshold:
                                threshold = chemical_threshold['threshold']
                            if chemical_threshold['active'] == True and 'threshold_per_linen' in chemical_threshold:
                                threshold_per_linen = chemical_threshold['threshold_per_linen']
                print(threshold)
                print(threshold_per_linen)
                print("after first")
                if threshold == 0:
                    flag_value = False
                else:
                    value += threshold
                if threshold_per_linen == 0:
                    flag_percent = False
                else:
                    percent += threshold_per_linen

            if th_type == 'water':
                threshold = 0
                threshold_per_linen = 0

                for i in site_threshold:

                    if 'consumption' in i:
                        water_threshold = i['consumption']['water_consumption'] if 'water_consumption' in i['consumption'] else None
                        if water_threshold != None:

                            if water_threshold['active'] == True and 'threshold' in water_threshold:
                                threshold = water_threshold['threshold']
                            if water_threshold['active'] == True and 'threshold_per_linen' in water_threshold:
                                threshold_per_linen = water_threshold['threshold_per_linen']
                print(threshold)
                print(threshold_per_linen)
                print("after first")
                if threshold == 0:
                    flag_value = False
                else:
                    value += threshold
                if threshold_per_linen == 0:
                    flag_percent = False
                else:
                    percent += threshold_per_linen

            if th_type=='water_costs': 
                threshold=0
                threshold_per_linen=0
               
                for i in site_threshold:
                    if 'cost' in i:
                        water_cost_threshold=i['cost']['water_costs'] if 'water_costs' in i['cost'] else None
                        if water_cost_threshold !=None:
                            if water_cost_threshold['active'] == True and 'threshold' in water_cost_threshold:
                                threshold=water_cost_threshold['threshold']
                            if water_cost_threshold['active'] == True and 'threshold_per_linen' in water_cost_threshold:
                                threshold_per_linen=water_cost_threshold['threshold_per_linen']
                print(threshold)
                print(threshold_per_linen)
                print("after first")
                if threshold==0:
                    flag_value = False
                else:
                     value += threshold
                if threshold_per_linen == 0:
                     flag_percent = False
                else:
                    percent += threshold_per_linen
                    
            if th_type=='energy_costs':
                threshold=0
                threshold_per_linen=0
                for i in site_threshold:
                    if 'cost' in i:
                        energy_cost_threshold=i['cost']['energy_costs'] if 'energy_costs' in i['cost'] else None
                        if energy_cost_threshold!=None:
                            if energy_cost_threshold['active'] == True and 'threshold' in energy_cost_threshold:
                                threshold=energy_cost_threshold['threshold']
                            if energy_cost_threshold['active'] == True and 'threshold_per_linen' in energy_cost_threshold:
                                threshold_per_linen=energy_cost_threshold['threshold_per_linen']
                print(threshold)
                print(threshold_per_linen)
                print("after first")
                if threshold==0:
                    flag_value = False
                else:
                     value += threshold
                if threshold_per_linen == 0:
                     flag_percent = False
                else:
                    percent += threshold_per_linen

            if th_type=='chemical_costs':
                threshold=0
                threshold_per_linen=0

                for i in site_threshold:
                    if 'cost' in i:
                        chemical_cost_threshold=i['cost']['chemical_costs'] if 'chemical_costs' in i['cost'] else None
                        if chemical_cost_threshold!=None:
                            if chemical_cost_threshold['active']==True and 'threshold' in chemical_cost_threshold:
                                threshold=chemical_cost_threshold['threshold']
                            if chemical_cost_threshold['active'] == True and 'threshold_per_linen' in chemical_cost_threshold:
                                threshold_per_linen=chemical_cost_threshold['threshold_per_linen']
                            if chemical_cost_threshold['active'] == True and 'units' in chemical_cost_threshold:
                                currency_symbol=chemical_cost_threshold['units']
                            
                print("chemical",threshold)
                print(threshold_per_linen)
                print(currency_symbol)
                print("after first")
                if threshold==0:
                    flag_value = False
                else:
                     value += threshold
                if threshold_per_linen == 0:
                     flag_percent = False
                else:
                    percent += threshold_per_linen
            
            if th_type == 'production':
                
                if event['groupby'] == 'week':
                    value = (7 * value) / len(sites)
                    return {'threshold': value}
                if event['groupby'] == 'hour':
                    value = value / (24 * len(sites))
                    return {'threshold': value}
                if event['groupby'] == 'day':
                    return {'threshold': value}
                if event['groupby'] == 'month':
                    value = (event['days'] * value) / len(sites)
                    return {'threshold': value}
            if th_type == 'rewash':
                th = calculate_thresholds_linen(
                    event, flag_value, flag_percent, value, percent)
                return th
            if th_type == 'water':
                th = calculate_thresholds_products(
                    event, flag_value, flag_percent, value, percent)
                return th
            if th_type == 'energy':
                th = calculate_thresholds_products(
                    event, flag_value, flag_percent, value, percent)
                return th
            if th_type == 'chemical':
                th = calculate_thresholds_products(
                    event, flag_value, flag_percent, value, percent)
                return th
            if th_type == 'aborted':
                th = calculate_thresholds_linen(
                    event, flag_value, flag_percent, value, percent)
                return th
            if th_type == 'water_costs':
                th = calculate_thresholds_products(
                    event, flag_value, flag_percent, value, percent)
                return th
            if th_type == 'energy_costs':
                th = calculate_thresholds_products(
                    event, flag_value, flag_percent, value, percent)
                return th
            if th_type == 'chemical_costs':
                th = calculate_thresholds_products(
                    event, flag_value, flag_percent, value, percent)
                return th

    except Exception as e:
        logger.error(e)
        logger.error("After e")


def lambda_handler(event, context):

    # TODO implement
    print(event)
    logger.info("After print")
    result = get_thresholds(event, event['threshold'], event['sites'])
    # if event['threshold'] == 'production':

    return result
