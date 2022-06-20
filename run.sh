python3 -m venv env
source env/bin/activate

pip install -r requirements.txt

uvicorn --port 8000 --host 0.0.0.0 main:app 
# uvicorn --ssl-certfile certificates/cert.pem --ssl-keyfile certificates/key.pem --port 3000 --host 0.0.0.0 main:app 
