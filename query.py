from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import requests

import datetime
from datetime import timedelta
import time
import pytz

import queries

ETHERSCAN_KEY = '**********************'

def get_blocks(days=30, hours=23, minutes=59, seconds=59, step=24):
    
    print("Getting block data...")

    today = pytz.utc.localize(datetime.datetime.utcnow()).date()
    snapshot_time =  pytz.utc.localize(datetime.time(hours, minutes, seconds))
    end_time = datetime.datetime.combine(today, snapshot_time)
    start_time = end_time - timedelta(days=days)
    step = step*60*60

    timestamps = list(range(int(start_time.timestamp()), int(end_time.timestamp()), step))

    blocks = []
    blocktimes = []

    for i, stamp in enumerate(timestamps):

        try:
            r = requests.get(f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={stamp}&closest=before&apikey={ETHERSCAN_KEY}').json()
            blocks.append(int(r['result']))
            blocktimes.append((int(r['result']), stamp))
        except:
            pass
        
        time.sleep(0.205) #Max 5 requests per second
    

    return blocks, blocktimes

def run_query(job, blocks=None, blocktimes=None):
    
    print("Querying subgraph...")
    
    # Select your transport with a defined url endpoint
    transport = AIOHTTPTransport(url="https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-mainnet")

    # Create a GraphQL client using the defined transport
    client = Client(transport=transport, fetch_schema_from_transport=True)

    responses = {}

    for i, block in enumerate(blocks):
        
        if blocks:
        
            params = {"block": block}

            result = client.execute(queries.QUERIES[job.query], variable_values=params)
        
        else:
            result = client.execute(queries.QUERIES[job.query])
            
        responses[block] = result[job.return_value]
        

    return responses