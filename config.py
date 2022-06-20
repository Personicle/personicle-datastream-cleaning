import os
import json
import pathlib
import configparser
from configparser import ConfigParser


__file_path = os.path.abspath(__file__)
__dir_path = os.path.dirname(__file_path)

PROJ_LOC=pathlib.Path(__dir_path)
AVRO_SCHEMA_LOC=os.path.join(PROJ_LOC, "avro_modules")

# Database url format
# dialect+driver://username:password@host:port/database
# postgresql+pg8000://dbuser:kx%25jj5%2Fg@pghost10/appdb

# IDENTITY_SERVICE_AUTH_URL, DATA_DICTIONARY_VERIFY_URL, EVENTHUB_CONFIG, WRITE_SERVICE_URL, WRITE_SERVICE_PORT
# DB_CREDENTIALS
if int(os.environ.get("INGESTION_PROD", '0')) != 1:
    print("in the dev environment")
    print("environment variables: {}".format(list(os.environ.keys())))

    __app_config = configparser.ConfigParser()
    __app_config.read(os.path.join(PROJ_LOC,'config.ini'))

    DATA_DICTIONARY_SERVER_SETTINGS = __app_config['DATA_DICTIONARY_SERVER']
    IDENTITY_SERVER_SETTINGS = __app_config['IDENTITY_SERVER']
    EVENTHUB_CONFIG = __app_config['EVENTHUB']
    PERSONICLE_SCHEMA_API = __app_config['PERSONICLE_SCHEMA_API']
    HOST_CONFIG = __app_config['DATA_WRITE_SERVICE']
    DB_CREDENTIALS = __app_config['CREDENTIALS_DATABASE']
    DELETE_USER= __app_config['DELETE_USER']
else:
    print("in the prod environment")
    try:
        DATA_DICTIONARY_SERVER_SETTINGS = {
            'HOST_URL': os.environ.get('DATA_DICTIONARY_SERVER_ENDPOINT', "0.0.0.0"),
            'HOST_PORT': os.environ.get('DATA_DICTIONARY_PORT', 5002)
        }

        IDENTITY_SERVER_SETTINGS = {
            'HOST_URL': os.environ.get('IDENTITY_SERVER_HOST', "0.0.0.0"),
            'HOST_PORT': os.environ.get('IDENTITY_SERVER_PORT', 5002),
            'PERSONICLE_AUTH_API_ENDPOINT': os.environ['PERSONICLE_AUTH_API_ENDPOINT']
        }
        DELETE_USER = {
            'API_ENDPOINT': os.environ['DELETE_USER_API_ENDPOINT'],
            'DELETE_USER_TOKEN': os.environ['DELETE_USER_TOKEN']
        }
        EVENTHUB_CONFIG = {
            'CONNECTION_STRING': os.environ['EVENTHUB_CONNECTION_STRING'],
            'EVENTHUB_NAME': os.environ['EVENTHUB_NAME'],
            'SCHEMA_REGISTRY_FQNS': os.environ['EVENTHUB_SCHEMA_REGISTRY_FQNS'],
            'SCHEMA_REGISTRY_GROUP': os.environ['EVENTHUB_SCHEMA_REGISTRY_GROUP'],
            'DATASTREAM_EVENTHUB_CONNECTION_STRING': os.environ['DATASTREAM_EVENTHUB_CONNECTION_STRING'],
            'DATASTREAM_EVENTHUB_NAME': os.environ['DATASTREAM_EVENTHUB_NAME']
        }

        HOST_CONFIG = {
            'HOST_URL': os.environ.get('DATA_WRITE_SERVER_HOST', "0.0.0.0"),
            'HOST_PORT': os.environ.get('DATA_WRITE_SERVER_PORT', 5004)
        }

        DB_CREDENTIALS = {
            "USERNAME": os.environ['CREDENTIALS_DB_USER'],
            "PASSWORD": os.environ['CREDENTIALS_DB_PASSWORD'],
            "HOST": os.environ['CREDENTIALS_DB_HOST'],
            "NAME": os.environ['CREDENTIALS_DB_NAME']
        }
        PERSONICLE_SCHEMA_API = {
             "MATCH_DICTIONARY_ENDPOINT": os.environ['DATA_DICTIONARY_SERVER_ENDPOINT']
        }
    except KeyError as e:
        print("failed to create configs for the application")
        print("missing configuration {} from environment variables".format(e))
        raise e

if os.environ.get('DEV_ENVIRONMENT', 'LOCAL') in ["PRODUCTION", "AZURE_STAGING"]:
    DB_CONFIG = {
        "USERNAME": os.environ["DB_USERNAME"],
        "PASSWORD": os.environ["DB_PASSWORD"],
        "HOST": os.environ["DB_HOST"],
        "NAME": os.environ["DB_NAME"]
    }

    OKTA_CONFIG = {
        "CLIENT_ID": os.environ["OKTA_CLIENT_ID"],
        "CLIENT_SECRET": os.environ["OKTA_CLIENT_SECRET"],
        "ISSUER": os.environ["OKTA_ISSUER"],
        "AUDIENCE": os.environ["OKTA_AUDIENCE"]
    }
    PERSONICLE_AUTH_API = {
        "ENDPOINT": os.environ["PERSONICLE_AUTH_API_ENDPOINT"]
    }

    PERSONICLE_SCHEMA_API = {
        "ENDPOINT": os.environ["PERSONICLE__API_ENDPOINT"]
    }

else:
    config_object = ConfigParser()
    config_object.read("config.ini")
    DB_CONFIG = config_object["CREDENTIALS_DATABASE"]
    OKTA_CONFIG = config_object["OKTA"]
    PERSONICLE_AUTH_API = config_object["PERSONICLE_AUTH_SERVICE"]
    PERSONICLE_SCHEMA_API = config_object["PERSONICLE_DATA_DICTIONARY"]
    EVENTHUB_CONFIG = config_object["EVENTHUB"]

