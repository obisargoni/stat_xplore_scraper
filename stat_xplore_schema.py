import requests
import pandas as pd 
import os



schema_url = 'https://stat-xplore.dwp.gov.uk/webapi/rest/v1/schema'

apikey = '65794a30655841694f694a4b563151694c434a68624763694f694a49557a49314e694a392e65794a7063334d694f694a7a644849756333526c6247786863694973496e4e3159694936496d39696153357a59584a6e623235705147396a63326b7559323875645773694c434a70595851694f6a45304f544d334e4449784e7a6773496d46315a434936496e4e30636935765a47456966512e4e666c776177773552754a76717a6a4a366f2d37344a4e745231412d66412d7679772d68394f427031576f'
schema_headers = {'APIKey':apikey}




def get_full_schema(schema_headers, url = schema_url, types_to_include = ["FOLDER","DATABASE","MEASURE","FIELD"], check_cache = False, schema_filename = '.\schema\schema.csv'):
    '''Get the schema information of all elements of the Stat-Xplore schema but sratting at the root 
    folder and iterating through the schema tree.

    Args:
        schema_headers (dict): The headers to use in the html request to the stat-xplore API.

    Kwargs:
        url (str): Defaults to the root schema folder: https://stat-xplore.dwp.gov.uk/webapi/rest/v1/schema
    '''

    # Get chema info for the root folder
    root_reponse = request_schema(url, schema_headers)
    if root_reponse['success'] == False:
        return
    root_json = root_reponse['response'].json()

    # Remove the 'children' key of the schema - we only want to record the 'id', 'type', 'label' and 'location' schema information
    del root_json['children']

    # Initialise full schema dataframe with the schema infomation of the root folder
    df_full_schema = pd.DataFrame([root_json])

    # Start loop to interate over all parent schema items
    still_to_map = df_full_schema
    while len(still_to_map) >0:

        new_schema = get_lower_tier_schema_from_upper_tier_schema(still_to_map, schema_headers, check_cache, cache_filename = schema_filename)

        # Only get children schemas desired types in the resulting schema. 
        # Eg exclude value sets such as all geographies (this can take a while to get)
        still_to_map = new_schema.loc[new_schema['type'].isin(types_to_include)]

        df_full_schema = pd.concat([df_full_schema, new_schema], join = 'outer')

        # Save the schema as we go
        df_full_schema.to_csv(schema_filename, index=False)

    return df_full_schema

        

def get_lower_tier_schema_from_upper_tier_schema(df_parent_schema, schema_headers, check_cache = False, cache_filename = '.\schema\schema.csv'):
    '''Function to loop through each of the parent elements of the upper tier schema and get the schema
    of the children of each one. Children schemas are combined together and returned.

    Args:
        df_parent_schema (pandas DataFrame): The parent schema of teh children schemas to return
        schema_headers (dict): The headers to use in the html request to the stat-xplore API.

    Kwargs:
        check_cache (bool): Default False. Check local directory for schema details.
        cache_filename (str): Default '.\schema\schema.csv'. The filename of the cached schema details 
                                to check for.
    '''
    # Get teh urls of each of the parent items
    parent_locations = df_parent_schema['location'].unique()

    # initialise the lower tier schema
    df_lower_tier_schema = pd.DataFrame()

    # Iterate over parent items and get children schema
    for location in parent_locations:
        children_schema_result = get_children_schema_of_url(location, schema_headers, check_cache, cache_filename)
        if children_schema_result['success'] == False:
            print('Faield to get children schema for location {}'.format(location))
            continue

        df_lower_tier_schema = pd.concat([df_lower_tier_schema, children_schema_result['schema']], join = 'outer')


    return df_lower_tier_schema

def get_children_schema_of_url(url, schema_headers, check_cache = False, cache_filename = '.\schema\schema.csv'):
    '''Given a url of a Stat-xplore schema item, get the schema details of the children (component) items. 
    The schema for each chils contains id, label, location(url) and type fields.The id of the parent element 
    s also included in the output schema.

    Args:
        url (str): The url of the schema item to get the children schema details of.
        schema_headers (dict): The headers to use in the html request to the stat-xplore API.

    Kwargs:
        check_cache (bool): Default False. Check local directory for schema details.
        cache_filename (str): Default '.\schema\schema.csv'. The filename of the cached schema details 
                                to check for.
    '''

    output = {'success':False, 'schema':None, 'from_cache':False}

    # Check for saved schema
    if (check_cache == True) & (os.path.exists(cache_filename) == True):

        try:
            df_full_schema = pd.read_csv(cache_filename)
            parent_id = df_full_schema.loc[ df_full_schema['location'] == url, 'id'].values[0]
            df_schema = df_full_schema.loc[df_full_schema['parent_id'] == parent_id]
            assert len(df_schema) != 0

            return {'success':True,'schema':df_schema, 'from_cache':True}
        except Exception as err:
            print(err)
            print('Unable to load cached schema. Requesting from API instead.')
            df_schema = pd.DataFrame()
    else:
        df_schema = pd.DataFrame()


    # If the schema dataframe is empty (which it will be if the cache wasn't successfully checked)
    # make request to the API to get the schema details
    if len(df_schema) == 0:

        schema_response = request_schema(url, schema_headers)
        if schema_response['success'] == False:
            return output

        # If this far, request to API was successfull and we should have schema information
        # Create dataframe with schema info of children elements
        schema_response_json = schema_response['response'].json()

        # Will there always be a children element?
        df_schema = pd.DataFrame(schema_response_json['children'])
        df_schema['parent_id'] = schema_response_json['id']

        return {'success':True,'schema':df_schema, 'from_cache':False}

def request_schema(url, schema_headers):
    '''Send request for schema to API. Check request was successful.

    Args:
        url (str): The url of the request.
        schema_headers (dict): The headers of the request.
    '''
    schema_response = requests.get(url, headers = schema_headers)

    # Check that request was successful. If not print message and exit.
    if schema_response.raise_for_status() is not None:
        print("Unsuccessful request to url:{}\nCheck url and API key.".format(url))
        print("Response status:\n{}".format(schema_response.raise_for_status()))
        return {'success':False, 'response':None}
    else:
        return {'success':True, 'response':schema_response}
