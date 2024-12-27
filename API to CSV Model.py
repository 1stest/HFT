from tradermade import stream
import pandas as pd
import json
import logging
from tabulate import tabulate
from IPython.display import display, HTML
import time
import threading

logging.basicConfig(level=logging.INFO)

LMDF = pd.DataFrame([{
    'Market': None,
    'Mid-Price': None,
    'Time-Stamp': None,
    'Bid': None,
    'Ask': None
}])

LMDF_Handle = display(HTML(tabulate(LMDF, headers='keys', tablefmt='html')),display_id=True)

stop_event = threading.Event()

def Org_Func(MDFY):
    global LMDF, LMDF_Handle
    try:
        if not MDFY.strip():
            logging.error("Received empty message.")
            return

        Parsing = json.loads(MDFY)

        if isinstance(Parsing, dict) and all(KCase in Parsing for KCase in ['symbol', 'mid', 'ts', 'bid', 'ask']):
            Pro_Dict = {
                'Market': Parsing['symbol'],
                'Mid-Price': float(Parsing['mid']),
                'Time-Stamp': int(Parsing['ts']),
                'Bid': float(Parsing['bid']),
                'Ask': float(Parsing['ask'])
            }

            for KCase, Value in Pro_Dict.items():
                LMDF.loc[0, KCase] = Value
 
            LMDF_Handle.update(HTML(tabulate(LMDF, headers='keys', tablefmt='html')))

            LMDF.to_csv('API_hub.csv', index=False)
        else:
            logging.error(f'Error: Unexpected data format received: {Parsing}')
    except json.JSONDecodeError as E2:
        logging.error(f'Error decoding JSON: {E2}')
    except Exception as E3:
        logging.error(f'Unexpected error: {E3}')

API_Key = #'API Key is entered here'
stream.set_ws_key(API_Key)
stream.set_symbols('NZDJPY')
stream.stream_data(Org_Func)
stream.connect()

# Uncomment to stop connection
# stop_event.set()
