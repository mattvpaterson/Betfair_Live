import requests
import json

#This function refreshes the API session for another 24h to prevent session_token expiry
def keep_alive(app_key,session_token):
    endpoint = "https://identitysso.betfair.com/api/keepAlive"
    header = { 'X-Application':app_key , 'X-Authentication' : session_token ,'content-type' : 'application/json' }
    response = requests.post(endpoint, headers=header)
    print('SessionKey refresh: '+str(response))

def get_call(app_key,session_token,call,filters):
    endpoint = "https://api.betfair.com/exchange/betting/rest/v1.0/"
    header = { 'X-Application':app_key , 'X-Authentication' : session_token ,'content-type' : 'application/json' }
    url = endpoint + call
    response = requests.post(url, data=filters, headers=header)
    result = response.json()
    return result

def test_get_call(app_key,session_token,call,filters):
    endpoint = "https://api.betfair.com/exchange/betting/rest/v1.0/"
    header = { 'X-Application':app_key , 'X-Authentication' : session_token ,'content-type' : 'application/json' }
    url = endpoint + call
    response = requests.post(url, data=filters, headers=header)
    print(response.json())
    
def lowercase_values(dictionary):
    return dict((k, str(v).lower()) for k,v in dictionary.items())

def dictionary_key_filter(dictionary,keep_keys):
    #general function to select only necessary key value pairs
    return {key: dictionary[key] for key in keep_keys}
