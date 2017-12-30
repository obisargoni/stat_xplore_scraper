# Functions to interact with the 'table' end point of the Stat-Xplore API
import pandas as pd 
import requests
import os
import stat_xplore_schema

table_url = 'https://stat-xplore.dwp.gov.uk/webapi/rest/v1/table'
APIKey = '65794a30655841694f694a4b563151694c434a68624763694f694a49557a49314e694a392e65794a7063334d694f694a7a644849756333526c6247786863694973496e4e3159694936496d39696153357a59584a6e623235705147396a63326b7559323875645773694c434a70595851694f6a45314d544d784f5441334e445573496d46315a434936496e4e30636935765a47456966512e2d4e4470356c64733266494f6d35584b4b7370394b48334f2d775542446f455248575f36467437477a3034'
table_headers = {'APIKey':APIKey,
                'Content-Type':'applciation/json'}
schema_headers = {'APIKey':APIKey}



def build_request_body(table_headers, schema_headers, measure_id, field_ids = None, df_schema = None):
def build_request_body(table_headers, schema_headers, measure_id, field_ids = None, df_schema = None, geog_folder_label = 'Geography (residence-based)', geog_field_label= 'National - Regional - LA - OAs', geog_level_label = 'Local Authority'):
    '''For an input measure ID and field IDs as well as the labels for the geography folder, field and level to get data for 
    build that dictionary of data to send to the Stat-Xplore table end point to request data.

    Args:
        table_headers (dict): The headers to uses for the request to the table endpoint of the Stat-Xplore API
        schema_headers (dict): The headers to uses for the request to the schema endpoint of the Stat-Xplore API
        measure_id (str): The id of the measure to request data for. This is the dataset, such as Attendence Allowance claimants, 
            that data is returned for

    Kwargs:
        field_ids (list of str, None): Default None. The field IDs of the fields to in intersect they data by
        df_schema (pandas DataFrame, None): Default to None. A DataFrame of the Stat-Xplore schema
        geog_folder_label (str): Defaults to 'Geography (residence-based)'. The label of the geography folder to get geography recodes from
        geog_field_label (str): Defaults tp 'National - Regional - LA - OAs'. The label of the geography field to get geography recodes from.
        geog_level_label (str): Defaults to 'Local Authority'. The label of the level (ie LAs, LSOAs etc) to get recodes for

    '''

    # Get database id
    database_id = 'str:database:' + measure_id.split(':')[-2]

    database_value = database_id

    measures_values = get_measures_request_body(measure_id)

    recodes_values = get_geography_recodes_request_body(schema_headers, database_id, geog_folder_label = geog_folder_label, geog_field_label= geog_field_label, geog_level_label = geog_level_label, df_schema = df_schema)

    dimensions_values = get_dimensions_body(schema_headers, database_id, field_ids, df_schema = df_schema)

    # Add in recode field id to the dimensions
    dimensions_values = dimensions_values + [[i] for i in list(recodes_values.keys())]

    body = {'database':database_value,
            'measures':measures_values,
            'recodes': recodes_values,
            'dimensions':dimensions_values}

    return body

def get_dimensions_body(schema_headers, database_id, field_ids, df_schema = None):
    '''Format the fields IDs to the required format for the dimensions section of the data request body. 
    If field IDs is None, get all available fields for the given database.

    Args:
        schema_headers (dict): The headers of the request.
        database_id (str): The ID of the database to get dimension fields for.
        field_ids (str or list of str or None): The fields to use as dimensions in the data request
        df_schema (pandas DataFrame or None): The stat-xplore schema

    Returns:
        list: The field ids to use as dimensions properly formatted

    '''

    if field_ids is None:
        field_ids_dict = stat_xplore_schema.get_database_fields(schema_headers, database_id, df_schema = df_schema)
        field_ids = list(field_ids_dict.values())

    field_ids = [field_ids] if isinstance(field_ids, str) else field_ids

    dimensions_array = [[i] for i in field_ids]

    return dimensions_array 

def get_measures_request_body(measure_ids):
    '''Given an input measure ID or list of measure IDs return a dictionary of the format:
    {'measures':['measure_id_1','measure_id_2', ...]}

    Args:
        measure_ids (str or list of str): The measure IDs to format

    Returns:
        list: A list of measure ids
    '''

    measure_ids = [measure_ids] if isinstance(measure_ids, str) else measure_ids

    return measure_ids

def get_geography_recodes_request_body(schema_headers, database_id, geog_folder_label = 'Geography (residence-based)', geog_field_label= 'National - Regional - LA - OAs', geog_level_label = 'Local Authority', df_schema = None, check_cache = False, schema_filename = '.\schema\schema.csv'):
    '''A wrapped function that gets the requested geography recodes and passes them to a functino that formats tha recodes into the
    dictionary format required for requesting data.
    
    Args:
        schema_headers (dict): The headers of the request
        database_id (str): The ID of the database to get dimension fields for
    
    Kwargs:
        geog_folder_label (str): Defaults to 'Geography (residence-based)'. The label of the geography folder to get geography recodes from
        geog_field_label (str): Defaults tp 'National - Regional - LA - OAs'. The label of the geography field to get geography recodes from
        geog_level_label (str): Defaults to 'Local Authority'. The label of the level (ie LAs, LSOAs etc) to get recodes for

    '''

    # Get the recodes
    recodes_dict = stat_xplore_schema.geography_recodes_for_geog_folder_geog_level(schema_headers, database_id, geog_folder_label, geog_field_label, geog_level_label, df_schema, check_cache, schema_filename)

    # Format the recodes
    recodes_data = format_recodes_for_api(recodes_dict, include_total = True)

    return recodes_data

def format_recodes_for_api(recodes_dict, include_total = True):
    '''Takes input dictionary of recode field id:list of desired re code values
    and formats this into a valid recodes object for reuesting table data from the API.
    A valid recodes objects is a dictionary with recode field ids as keys. Each id looks to 
    another dictionary oject with a 'map' key and a 'total' key. Desired recodes are set as an array
    of values, each in their own array, within the map object.

    EG

    'field_id':{  'map':[
                    ['field_value_1'],
                    ['field_value_2'],
                    ['field_value_3']
                    ],
                'total': true}
    Args:
        recodes_dict (dict): A dictionary with the recode field id as a key and a list of recode field
            values as the item.

    Kwargs:
        include_total (bool): Default False. Set whether or not to set the 'total' option to 'true' or 'false'
            This controls whether data returned by the request includes a total across all recode field values.

    Returns:
        dict: Recode field id and field values in the required format
    '''

    # Assumes there is only one recode field id
    field_id = list(recodes_dict.keys())[0]

    field_values = recodes_dict[field_id]

    # Convert values into an array of arrays
    map_values = [ [i] for i in field_values ]

    if include_total == False:
        total_value = False
    else:
        total_value = True
    # Combine into single dict
    recodes_data = {field_id:{
                                'map': map_values,
                                'total':total_value
                            }
                    }
    #  Might need to convert into json object (ie just text)
    return recodes_data

def request_table(url, table_headers, table_data):
    '''Send request for table to API. Check request was successful.

    Args:
        url (str): The url of the request.
        table_headers (dict): The headers of the request.
    '''
    table_response = requests.post(url, headers = table_headers, data = table_data)

    # Check that request was successful. If not print message and exit.
    if table_response.raise_for_status() is not None:
        print("Unsuccessful request to url:{}\nCheck url and API key.".format(url))
        print("Response status:\n{}".format(table_response.raise_for_status()))
        return {'success':False, 'response':None}
    else:
        return {'success':True, 'response':table_response}
