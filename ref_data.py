import requests
import json
import sqlite3
import pandas as pd

class ref_data:
    
    def __init__(self,app_key,session_token):
        self.app_key = app_key
        self.session_token = session_token
    
    #This function refreshes the API session for another 24h to prevent session_token expiry
    def keep_alive(self):
        endpoint = "https://identitysso.betfair.com/api/keepAlive"
        header = { 'X-Application':self.app_key , 'X-Authentication' : self.session_token ,'content-type' : 'application/json' }
        response = requests.post(endpoint, headers=header)
        print('SessionKey refresh: '+str(response))

    def test_get_call(self,call,filters):
        endpoint = "https://api.betfair.com/exchange/betting/rest/v1.0/"
        header = { 'X-Application':self.app_key , 'X-Authentication' : self.session_token ,'content-type' : 'application/json' }
        url = endpoint + call
        response = requests.post(url, data=filters, headers=header)
        print(response.json())

    def get_call(self,call,filters):
        endpoint = "https://api.betfair.com/exchange/betting/rest/v1.0/"
        header = { 'X-Application':self.app_key , 'X-Authentication' : self.session_token ,'content-type' : 'application/json' }
        url = endpoint + call
        response = requests.post(url, data=filters, headers=header)
        result = response.json()
        return result
    
    def find_events(self):
        events = self.get_call("listEvents/",'{"filter":{"eventTypeIds":[7],"marketTypeCodes":["WIN"],"marketCountries":["GB","IE"]}}')
        events = [ref_data.dictionary_key_filter(event['event'],['id','countryCode','venue','openDate']) for event in events]
        events = [ref_data.lowercase_values(event) for event in events]

        for event in events:
            event['openDate'] = event['openDate'][:10]
            event = ref_data.lowercase_values(event)
            
        self.events = events
        
    def establish_database_connection(self,database):
        self.connection = sqlite3.connect(database)
        self.cursor = self.connection.cursor()
        self.insert_race_query = "INSERT INTO races VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        self.insert_runner_query = "INSERT INTO runners VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
        print("connection established")
    
    def create_races_table(self):
        create_table = ("CREATE TABLE races (event_id int, market_id real, country text, venue text, name text, date text," +
                    "time text, number_runners int, type text , going text, yards int, seconds real, total_vol real, private_vol real," + 
                    "pnl real, order_count int, bet_count int)")
        self.cursor.execute(create_table)
        print('races table created')
        
    def create_runners_table(self):
        create_table = ("CREATE TABLE runners (market_id real, selection_id int, runner_name text, weight_lbs real," +
                "jockey text, individual_speed real, win_lose int, bsp real, ppwap real, ppmin real," +
                "ppmax real, pptradedvol real)")
        self.cursor.execute(create_table)
        print('runners table created')

    def add_races(self):
        for event in self.events:
            event_id = event['id']
            result = self.get_call("listMarketCatalogue/",'{"filter":{"eventIds": ["'+event_id+'"]},"maxResults": "100","marketProjection": ["COMPETITION","EVENT","EVENT_TYPE","RUNNER_DESCRIPTION","MARKET_START_TIME"]}')
            #checking of the standard name format 2fxxxx or 3mxxxx
            lst = [market for market in result if market['marketName'][0].isnumeric()*market['marketName'][1].isalpha()] 
            lst = [ref_data.market_cleaner(market) for market in lst]
            for market in lst: #seperating marketStartTime into Date & Time
                market['marketdate'] = market['marketStartTime'][:10]
                market['marketStartTime'] = market['marketStartTime'][11:19]
                market['marketName'] = market['marketName'].lower()

            for market in lst:
                race = (event['id'],market['marketId'],event['countryCode'],event['venue'], market['marketName'],market['marketdate'],market['marketStartTime'],len(market['runners']),None,None,None,None,None,None,None,None,None)
            self.cursor.execute(self.insert_race_query, race)
        
    def add_runners(self):
        for event in self.events:
            event_id = event['id']
            result = self.get_call("listMarketCatalogue/",'{"filter":{"eventIds": ["'+event_id+'"]},"maxResults": "100","marketProjection": ["COMPETITION","EVENT","EVENT_TYPE","RUNNER_DESCRIPTION","MARKET_START_TIME"]}')
            #checking of the standard name format 2fxxxx or 3mxxxx
            lst = [market for market in result if market['marketName'][0].isnumeric()*market['marketName'][1].isalpha() ] 
            lst = [ref_data.market_cleaner(market) for market in lst]
            lst = [ref_data.dictionary_key_filter(market,['marketId','runners']) for market in lst]
        
            for market in lst:
                for runner in market['runners']:
                    record = (market['marketId'], runner['selectionId'], runner['runnerName'].lower(),None,None,None,None,None,None,None,None,None)
                    self.cursor.execute(self.insert_runner_query, record)
                    
    def delete_duplicates(self):
        # Finding & deleting runner duplicates on duplicate market_id & selection_id
        try:
            pd.read_sql_query("DELETE FROM runners WHERE EXISTS (SELECT 1 FROM runners p2 WHERE runners.market_id = p2.market_id AND runners.selection_id = p2.selection_id AND runners.rowid > p2.rowid);", self.connection)
        except:
            pass
    
        # Finding & deleting race duplicates on duplicate market_id
        try:
            pd.read_sql_query("DELETE FROM races WHERE EXISTS (SELECT 1 FROM races p2 WHERE races.market_id = p2.market_id AND races.rowid > p2.rowid);", self.connection)
        except:
            pass
    
    
    @staticmethod
    def lowercase_values(dictionary):
        return dict((k, str(v).lower()) for k,v in dictionary.items())

    @staticmethod
    def dictionary_key_filter(dictionary,keep_keys):
        #general function to select only necessary key value pairs
        return {key: dictionary[key] for key in keep_keys}

    @staticmethod
    def market_cleaner(example):
        #keeps on the fields we want in the market dict and runners sub_dict for market storage
        clean = ref_data.dictionary_key_filter(example,['marketId','marketName','marketStartTime','runners'])
        clean['runners'] = [ref_data.dictionary_key_filter(runner,['selectionId','runnerName']) for runner in clean['runners']]
        clean['runners'] = [ref_data.lowercase_values(runner) for runner in clean['runners']]
        return clean

    @staticmethod
    def market_name_check(market_name):
        #check for main markets with the standard name format e.g. '2m1f Hcap' not 'To Be Placed'
        return market_name[0].isnumeric()*market_name[1].isalpha()
