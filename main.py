from inspect import trace
from typing import List, Optional, Text
from urllib.parse import uses_fragment
from fastapi import FastAPI, status, Request, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
import os
from datetime import datetime, timezone
from db_connection import *
import json
from base_schema import base_schema
from sqlalchemy import select
from fastapi.security import OAuth2PasswordBearer
from configparser import ConfigParser
from okta_jwt.jwt import validate_token as validate_locally
from okta_jwt_verifier import AccessTokenVerifier, IDTokenVerifier
from fastapi.responses import JSONResponse
import httpx
import copy
from config import PERSONICLE_AUTH_API, PERSONICLE_SCHEMA_API
import requests
# config_object = ConfigParser()
# config_object.read("config.ini")
# OKTA_CONFIG = config_object["OKTA"]

import logging

if os.environ.get("FILE_LOGGING", '0') == '1':
    from logging.config import fileConfig
    fileConfig("logging.cfg")

LOG = logging.getLogger(__name__)


app = FastAPI(title="Personicle backend data api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


script_dir = os.path.dirname(__file__)
data_dict_path = os.path.join(script_dir,"data_dictionary/personicle_data_types.json")

with open(data_dict_path, 'r') as fi:
            personicle_data_types_json = json.load(fi)

# oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')

    
def match_data_dictionary(stream_name):
    """
    Match a data type to the personicle data dictionary
    returns the data type information from the data dictionary
    """
    # stream_name = stream_name[:stream_name.rfind('.')]
    data_stream = personicle_data_types_json["com.personicle"]["individual"]["datastreams"][stream_name]
    return data_stream

def get_table_name(data_type):
    personicle_data_type = data_type.split(".")
    return personicle_data_types_json["com.personicle"]["individual"]["datastreams"][personicle_data_type[-1]]["TableName"] 

    
# @app.post('/token')
# def login(request: Request):
#     return retrieve_token(
#         request.headers['authorization'],
#          '{}'.format(okta["ISSUER"]),
        
#     )
# can be run on multiple workers with uvicorn.workers.UvicornWorker
@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

@app.get("/")
async def test_connection(request: Request):
    return "Testing data apis"

@app.get("/datastreams")
async def get_data(request: Request, datatype: str, startTime=str,endTime=str, source: Optional[str] = None, authorization = Header(None)):
    try:
        authorized, response = await is_authorized(authorization,datatype)

        if response['message'] == 'INVALID_SCOPES':
            return JSONResponse(status_code=status.HTTP_403_FORBIDDEN, content="You do not have access to requested scopes")

        if  authorized:
            try:
                user_id = response['user_id']
                # send request to validation server to get datastream info
                is_success, stream_information = await find_data_schema(authorization, datatype)

                if is_success == False:
                    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content='Could not match requested data type with data dictionary')
                # stream_information = match_data_dictionary(datatype)
                print("{}".format(stream_information))
                table_name = stream_information['TableName']
                model_class = generate_table_class(table_name, copy.deepcopy(base_schema[stream_information['base_schema']]))

                # query = (heartrates.select().where(heartrates.c.timestamp.between(datetime.strptime(startTime,'%Y-%m-%dT%H:%M:%S.%f'),datetime.strptime(endTime,'%Y-%m-%dT%H:%M:%S.%f')) 
                # & (heartrates.c.individual_id == user_id) & (heartrates.c.source == source ))) if source else (heartrates.select().where(heartrates.c.timestamp.between(datetime.strptime(startTime,'%Y-%m-%dT%H:%M:%S.%f'),datetime.strptime(endTime,'%Y-%m-%dT%H:%M:%S.%f')) 
                # & (heartrates.c.individual_id == user_id))) 
                query = (select(model_class).where((model_class.individual_id == user_id) & (model_class.source == source) & 
                (model_class.timestamp.between(datetime.strptime(startTime,'%Y-%m-%d %H:%M:%S.%f'),datetime.strptime(endTime,'%Y-%m-%d %H:%M:%S.%f'))))) if source else (select(model_class).where((model_class.individual_id == user_id) & 
                (model_class.timestamp.between(datetime.strptime(startTime,'%Y-%m-%d %H:%M:%S.%f'),datetime.strptime(endTime,'%Y-%m-%d %H:%M:%S.%f'))))) 

                return await database.fetch_all(query)
            except Exception as e:
                # print(e)
                LOG.error(traceback.format_exc())
                return "Invalid request", 422
        else:
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content="Invalid Bearer token")
    except Exception as e :
        print(e)
        return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content="Bearer token not present in request")

# @app.get('/')
# async def get_data():
#     return {"message": "Hello from personicle"}

async def find_data_schema(authorization, datatype):
    async with httpx.AsyncClient(verify=False) as client:
        headers = {'Authorization': f'{authorization}'}
        params = {'data_type': 'datastream','stream_name': datatype}

        schema_response = await client.get(PERSONICLE_SCHEMA_API['MATCH_DICTIONARY_ENDPOINT'],params=params,headers=headers)
        # authorization = await client.get("http://127.0.0.1:5000/authenticate",params=params,headers=headers)

        return schema_response.is_success, schema_response.json()

async def is_authorized(authorization,datatype):
    async with httpx.AsyncClient(verify=False) as client:
        headers = {'Authorization': f'{authorization}'}
        params = {'scopes': datatype}

        authorization = await client.get(PERSONICLE_AUTH_API['ENDPOINT'],params=params,headers=headers)
        # authorization = await client.get("http://127.0.0.1:5000/authenticate",params=params,headers=headers)

        return authorization.is_success, authorization.json()

@app.get("/events")
async def get_events_data(request: Request, startTime: str,endTime: str, source: Optional[str] = None, event_type: Optional[str]=None, authorization = Header(None)):
    try:

        authorized, response = await is_authorized(authorization,"events.read")
        if response['message'] == 'INVALID_SCOPES':
            return JSONResponse(status_code=status.HTTP_403_FORBIDDEN, content="You do not have access to read events")

        if authorized:
            try:
                # stream_information = match_event_dictionary(event_type)
                # table_name = stream_information['TableName']
                user_id = response['user_id']
                table_name = "personal_events"
                model_class = generate_table_class(table_name, copy.deepcopy(base_schema["event_schema.avsc"]))

                sources = source.split(";") if source is not None else None
                event_types = event_type.split(";") if event_type is not None else None

                LOG.info("Event request received for user: {}, from: {}, to: {}, source: {}, event_type: {}".format(user_id, startTime, endTime, source, event_type))

                if event_types is not None and sources is not None:
                    query = (select(model_class).where((model_class.user_id == user_id) & (model_class.source.in_(sources)) & 
                    (model_class.start_time.between(datetime.strptime(startTime,'%Y-%m-%d %H:%M:%S.%f'),datetime.strptime(endTime,'%Y-%m-%d %H:%M:%S.%f'))) & 
                    (model_class.event_name.in_(event_types))))
                elif event_types is not None:
                    query = (select(model_class).where((model_class.user_id == user_id) &  
                    (model_class.start_time.between(datetime.strptime(startTime,'%Y-%m-%d %H:%M:%S.%f'),datetime.strptime(endTime,'%Y-%m-%d %H:%M:%S.%f'))) & 
                    (model_class.event_name.in_(event_types))))
                elif sources is not None:
                    query = (select(model_class).where((model_class.user_id == user_id) &  (model_class.source.in_(sources)) & 
                    (model_class.start_time.between(datetime.strptime(startTime,'%Y-%m-%d %H:%M:%S.%f'),datetime.strptime(endTime,'%Y-%m-%d %H:%M:%S.%f')))))
                else:
                    query = (select(model_class).where((model_class.user_id == user_id) &
                    (model_class.start_time.between(datetime.strptime(startTime,'%Y-%m-%d %H:%M:%S.%f'),datetime.strptime(endTime,'%Y-%m-%d %H:%M:%S.%f')))))
                    

                return await database.fetch_all(query)
            except Exception as e:
                # print(e)
                LOG.error(traceback.format_exc())
                return "Invalid request", 422
        else:
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content="Invalid Bearer token")
    except Exception as e :
        return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content="Bearer token not present in request")
