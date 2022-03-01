## based on https://github.com/i4Ds/SDOBenchmark/blob/master/dataset/data/extract.py
import datetime as dt
import logging
import os
from typing import Iterable, Optional, Tuple
import pandas as pd
import requests
import util as util

#logger = logging.getLogger(__name__)

def load_hek_data(start_datetime: dt.datetime, end_datetime: dt.datetime, param_json: str = 'query_params.json',npages: int=1)-> Iterable[dict]:
    page = 1
    params=pd.read_json(param_json).iloc[0].to_dict()
    params["event_starttime"]= start_datetime.strftime(util.HEK_DATE_FORMAT)
    params["event_endtime"]= end_datetime.strftime(util.HEK_DATE_FORMAT)
    sess = util.requests_retry_session()
    while True:
        r = sess.get("http://www.lmsal.com/hek/her", params=params)
        events = r.json()["result"]

        if len(events) == 0:
            break

        end_date = None
        for event in events:
            end_date = util.hek_date(event["event_endtime"])
            yield event

        print(f"Loaded page {params['page']}, last date was {end_date}")
        params['page'] += 1
        if params['page']>npages:
            break

def extract_events(raw_events: List[dict], event_type:str = 'FL', event_source:str='SSW Latest Events') -> Iterable[dict]:
    '''for a given event type and data source, return a generator of queries that match '''
    for i,event in enumerate(raw_events):
        print(i)
        event['starttime'] = util.hek_date(event["event_starttime"])
        event['endtime'] = util.hek_date(event["event_endtime"])
        if event["event_type"] == "FL":
            event['peaktime'] = util.hek_date(event["event_peaktime"])
        else:
            event['peaktime']=(event['endtime']-event['starttime'])/2 + event['starttime'].date()#midpoint in time
        yield event
