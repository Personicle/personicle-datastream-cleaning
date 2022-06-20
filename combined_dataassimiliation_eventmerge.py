# -*- coding: utf-8 -*-
"""
Created on Tue May 17 02:35:12 2022

@author: nbadam
"""

# -*- coding: utf-8 -*-
"""
Created on Tue May 17 02:20:57 2022

@author: nbadam
"""


import os
import numpy as np
import pandas as pd
from flask import Flask
from flask import request
from sqlalchemy import select
import pandas.io.sql as sqlio
from base_schema import *
from db_connection import *
import json,time
from pandarallel import pandarallel
pandarallel.initialize()
from producer.send_datastreams_to_azure import datastream_producer
from producer import send_records_azure
from config import *

#########

app = Flask(__name__)




@app.route('/fetch_data/',methods=['GET'],endpoint='request_page_fetchdata')

def request_page_fetchdata():
    
    user  = request.args.get('user',type=str , default='')
    tblname  = request.args.get('data', type=str ,default='')
    source  = request.args.get('source',type=str , default='')
    
    sources = source.split(";") if source is not None else None
   
    ts  = request.args.get('starttime',type=str , default='')
    te  = request.args.get('endtime',type=str , default='')
    freq  = request.args.get('freq',type=str , default='')

    query_hr='select * from '+ tblname
    
    
    hr= sqlio.read_sql_query(query_hr,engine)
   
   
    hr=hr[(hr.individual_id==user)&(hr.source.isin(sources))&(pd.to_datetime(hr.timestamp)>=ts)&(pd.to_datetime(hr.timestamp)<=te)].copy()
    
     
    hr['startdate']=hr['timestamp']-pd.Timedelta('1min')
    hr.rename(columns={'timestamp':'enddate','source':'sourcename'},inplace=True)
    hr=hr[['individual_id','startdate','enddate', 'sourcename', 'value', 'unit', 'confidence']].copy()
    
    
    
    hr.drop_duplicates(inplace=True)
    hr.reset_index(inplace=True)
    hr.rename(columns={'index':'ind'},inplace=True)
    hr['diff_seconds'] = hr['enddate'] - hr['startdate']
    hr['diff_seconds']=hr['diff_seconds']/np.timedelta64(1,'s')
    hr['weight']=np.round(hr['diff_seconds']**(-1),4)
    hr.weight.fillna(1,inplace=True)
    
         
    import pandas as pd1
    
    hr['startdate']=hr.parallel_apply(lambda d:pd1.date_range(d['startdate'],d['enddate']-pd1.Timedelta(freq) if d['diff_seconds'] > 0 else d['startdate'],freq=freq),axis=1)
    hr=hr.explode('startdate')
    hr['enddate']=(hr['startdate']+pd1.Timedelta(freq)).dt.ceil('1min')
    hr['wt_value']=hr['weight']*hr['value']
    hr=hr.pivot_table(index=['individual_id','startdate','enddate'],columns='sourcename',values=['wt_value','weight']).reset_index()
    hr.columns = list(map("_".join, hr.columns))
    hr.rename(columns={'individual_id_':'individual_id','startdate_':'startdate','enddate_':'enddate'},inplace=True)
    hr.fillna(0,inplace=True)
       
    
    
    hr['result_stream']=np.round((hr.loc[:,hr.columns.str.startswith("wt_value")].sum(axis=1))/(hr.loc[:,hr.columns.str.startswith("weight")].sum(axis=1)))


    for col in ('startdate','enddate'):    
         hr[col] = hr[col].dt.strftime('%Y-%m-%d %H:%M:%S')
   
    
    hr=hr.loc[:,~hr.columns.str.startswith('wt_value')].copy()
    
    
    
    
    hr=pd.melt(hr, id_vars=['individual_id','startdate','enddate','result_stream'], 
                  value_vars=hr.columns[hr.columns.str.contains('weight')]).copy()
    
    
    hr=hr[hr.value!=0]
    hr['final_source']=hr['variable'].astype(str)+':' +hr['value'].astype(str)
    
    hr.drop(columns=['variable','value'],inplace=True)
    
    hr=hr.groupby(['individual_id','startdate','enddate','result_stream'])['final_source'].apply(';'.join).reset_index()
    

    
    print("hr dim:", hr.shape)
    data=hr[['individual_id','startdate','enddate','final_source','result_stream']].copy()
    
    data.rename(columns={'result_stream':'value','final_source':'Personicle'},inplace=True)


    data['Personicle'] = data['Personicle'].str.replace('weight_','')
    #data=hr.to_dict('index')
    data=data.to_json(orient='records')
    total_data_points=len(data)
    print(total_data_points)
    
    
   
    try:
        datastream_producer(data)
    except Exception as e:
    
            #logging.info("Total data points added for source {}: {}".format(datasource, total_data_points))
        logging.info("Total data points added for source {}: {}".format(data, total_data_points))
        logging.error(traceback.format_exc())
           
    
    
    return data




@app.route('/merge_data/',methods=['GET'],endpoint='request_page_eventmerge')


def request_page_eventmerge():
    
    user  = request.args.get('user',type=str , default='')
    tblname  = request.args.get('data', type=str ,default='')
    source  = request.args.get('source',type=str , default='')
    
    sources = source.split(";") if source is not None else None
    ts  = request.args.get('starttime',type=str , default='')
    te  = request.args.get('endtime',type=str , default='')

    query='select * from '+ tblname
    
    
    events= sqlio.read_sql_query(query,engine)
    

    events=events[(events.user_id==user)&(events.source.isin(sources))&(pd.to_datetime(events.start_time)>=ts)&(pd.to_datetime(events.end_time)<=te)].copy()
    
        
    events=events[['user_id','start_time','end_time','event_name','source','parameters']].sort_values(by='start_time').copy()
    
    
    for col in ['start_time','end_time']:
       
        events[col] = pd.to_datetime(events[col])
        events[col] = events[col].values.astype('<M8[m]')
        
    data=events.copy()
    
    
    data=events.sort_values(by=['start_time']).copy()

    data["end_time_lag"] = data['end_time'].shift()
    def merge(x):
        
        if (x.start_time <=x.end_time_lag):
            x.end_time = max(x.end_time, x.end_time_lag)
           
        return x
    
    res = pd.DataFrame([])
    for k,grp in (data.groupby(['user_id','event_name','source'],as_index=False)):
        
        res = pd.concat([grp.apply(merge, axis=1).groupby(['user_id','event_name','source'],as_index=False).apply(lambda e: e.assign(
            grp=lambda d: (
                ~(d["start_time"] <= (d["end_time"].shift()))
                ).cumsum())), res])
    
    data = res.groupby(['user_id','event_name','grp'],as_index=False).agg({"start_time": "min", "end_time": "max"
                                                                                    ,'parameters': list,
                                                                                    'source':';'.join})

    
    def param_append(list1,total_duration):
        dct={}
        totalcaloriesburned=0
        for k,v in enumerate(list1):
            #v=v.replace("'",'"')
            #v=json.loads(v)
            dct['param'+str(k)]=v
            
            if 'caloriesBurned' in v:
                totalcaloriesburned+=v['caloriesBurned']
    
        if totalcaloriesburned!=0:
            dct['totalcaloriesburned']=totalcaloriesburned
         
        dct['duration']=np.int(total_duration*60000)
    
        return dct
                                                                                      
    data.drop(columns='grp',inplace=True)
    
    
    data['duration_minutes'] = data['end_time'] - data['start_time']
    data['duration_minutes']=data['duration_minutes']/np.timedelta64(1,'m')
    
    
    lst=[]
    for i in range(data.shape[0]):
        temp=param_append(data.iloc[i,data.columns.get_loc('parameters')],data.iloc[i,data.columns.get_loc('duration_minutes')])
        lst.append(temp)
        
    
    data['parameter2']=lst
    
   
    data['parameter2'] = data['parameter2'].apply(lambda x: json.dumps(x))
    
        
    data=data[['user_id', 'start_time', 'end_time', 'event_name', 'source','parameter2']].copy()
    
    data.rename(columns={'parameter2':'parameters','user_id':'individual_id'},inplace=True)
    
    for col in ('start_time','end_time'):     
        data[col] = data[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        

    data['source'] = 'Personicle;' + data['source'].astype(str)
    
    
    data=data.to_dict(orient='records')
    #print("datatypeeeeeeeeeeeeeee:",type(data))


    total_data_points=len(data)
    data1=json.dumps(data)
    

    
    try:
        send_records_azure.send_records_to_eventhub('event_schema.avsc', data, EVENTHUB_CONFIG['EVENTHUB_NAME'])
        return {"success": True, "number_of_records": total_data_points}
    except Exception as e:
        logging.error(traceback.format_exc())
        return {"success": False, "error": e}
    
    
    return data1





if __name__ == '__main__':
     app.run(port=7777)



