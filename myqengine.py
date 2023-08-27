import time
import os
import pandas as pd
import pyarrow as pa
from fds.analyticsapi.engines.api_client import ApiClient
from fds.analyticsapi.engines.api.quant_calculations_api import QuantCalculationsApi
from fds.analyticsapi.engines.model.quant_calculation_parameters_root import QuantCalculationParametersRoot
from fds.analyticsapi.engines.model.quant_calculation_parameters import QuantCalculationParameters
from fds.analyticsapi.engines.model.quant_calculation_meta import QuantCalculationMeta
from fds.analyticsapi.engines.model.quant_screening_expression import QuantScreeningExpression
from fds.analyticsapi.engines.model.quant_fql_expression import QuantFqlExpression
from fds.analyticsapi.engines.configuration import Configuration
from fds.analyticsapi.engines.model.quant_screening_expression_universe import QuantScreeningExpressionUniverse
from fds.analyticsapi.engines.model.quant_identifier_universe import QuantIdentifierUniverse
from fds.analyticsapi.engines.model.quant_fds_date import QuantFdsDate
from fds.analyticsapi.engines.model.quant_date_list import QuantDateList
from urllib3 import Retry
import os
from dotenv import load_dotenv
load_dotenv()

host ="https://api.factset.com"
fds_username = os.getenv("FACTSET_USERNAME")
fds_api_key = os.getenv("FACTSET_API_KEY")

"""
Initialize configuraiton object for Analytics API v3
Force 3 retrys when response times out or hits rate limit
"""
fds_config = Configuration(
    host="https://api.factset.com",
    username=fds_username,
    password=fds_api_key,
)
# 429 -> Max Requests
# 503 -> Requet Timed Out

## SSL verification
fds_config.verify_ssl=True

fds_config.retries = Retry(
    total=3,
    status=3,
    status_forcelist=frozenset([429, 503]),
    backoff_factor=2,
    raise_on_status=False,
    
)
#connect to api
api_client = ApiClient(fds_config)

class screen_universe:
    def __init__(self,universe_expr,universe_type):
        self.universe_expr = universe_expr
        self.universe_type= universe_type
        self.source = 'ScreeningExpressionUniverse'
        self.security_expr = 'TICKER'
    def __str__(self):
        return self
    def get_univ(self):
        return QuantScreeningExpressionUniverse(universe_expr = self.universe_expr,universe_type = self.universe_type,source= self.source,security_expr = self.security_expr)

class id_universe:
    def __init__(self,ids,universe_type):
        self.ids = ids
        self.universe_type= universe_type
        self.source = 'IdentifierUniverse'
    def __str__(self):
        return self
    def get_univ(self):
        return QuantIdentifierUniverse(identifiers = self.ids,universe_type = self.universe_type,source= self.source)
class time_series:
    def __init__(self,start_date,end_date = '0',frequency = 'M',calendar = 'NAY'):
        self.start_date = start_date
        self.end_date= end_date
        self.frequency = frequency
        self.calendar = calendar
    def __str__(self):
        return self
    def get_dates(self):
            return QuantFdsDate(source = 'FdsDate',start_date = self.start_date,end_date = self.end_date,frequency=self.frequency,calendar=self.calendar)

class post_calc:
    def __init__(self,response):
        self.data = response[0]
        self.metadata = response[1]
    def __str__(self):
        return self
    
def get_data(calc_id, calc_unit_id):
    # Get the calculation data
    response = QuantCalculationsApi(api_client).get_calculation_unit_result_by_id(id=calc_id, unit_id=calc_unit_id)
    reader = pa.BufferReader(response[0].read())
    return pd.read_feather(reader)


def get_metadata(calc_id, calc_unit_id):
    response = QuantCalculationsApi(api_client).get_calculation_unit_info_by_id(id=calc_id, unit_id=calc_unit_id)
    reader = pa.BufferReader(response[0].read())
    return pd.read_feather(reader)


def get_results(response):
    '''Poll until the data is calculated. Return data and metadata'''
    data = None
    metadata = None

    if response[1] == 201:
        print('todo: support instant response')
    else:
        # Get the calculation id
        calc_id = response[0].data.calculationid

        # Check the status
        status_rsp = QuantCalculationsApi(api_client).get_calculation_status_by_id(id=calc_id)

        # Poll for status updates until it's not 'Queued' or 'Executing'
        while status_rsp[1] == 202 and (status_rsp[0].data.status in ('Queued', 'Executing')):
            max_age = '5'
            age_value = status_rsp[2].get('cache-control')
            if age_value is not None:
                max_age = age_value.replace('max-age=', '')
            time.sleep(int(max_age))
            status_rsp = QuantCalculationsApi(api_client).get_calculation_status_by_id(id=calc_id)

        # Get the results
        for (calc_unit_id, calc_unit) in status_rsp[0].data.units.items():
            # Was the calculation successful?
            if calc_unit.status == 'Success':
                data = get_data(calc_id, calc_unit_id)
                metadata = get_metadata(calc_id, calc_unit_id)
            else:
                print('Error message : ' + str(calc_unit.errors))

    return (data, metadata)


def calculate(universe, dates, formulas,source = 'ScreeningExpression',is_array_return_type = False):
    quant_formulas = []
    for formula in formulas:
        if(source=='ScreeningExpression'):
            quant_formulas.append(QuantScreeningExpression(expr=formula,name=formula,source = source))
        else:
            quant_formulas.append(QuantFqlExpression(expr=formula,name=formula,source = source,is_array_return_type= is_array_return_type))
    params = QuantCalculationParametersRoot(
        data={'1': QuantCalculationParameters(universe=universe.get_univ(), dates=dates.get_dates(), formulas=quant_formulas)},
        meta=QuantCalculationMeta(format='Feather'),
    )
    response = QuantCalculationsApi(api_client).post_and_calculate(quant_calculation_parameters_root=params)
    rep = get_results(response)
    return post_calc(rep)


