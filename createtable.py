from sqlalchemy import JSON, TIMESTAMP, Table, Column, Integer, String, MetaData, create_engine
meta = MetaData()

engine = create_engine('postgresql://{}:{}@{}/{}?sslmode={}'.format("tirth", "password", "localhost", "personicletest", 'prefer'))

personal_events = Table(
   'personal_events', meta, 
   Column('individual_id', String, primary_key = True), 
   Column('start_time', TIMESTAMP,primary_key=True), 
   Column('end_time', TIMESTAMP), 
   Column('event_name', String), 
   Column('parameters', JSON), 

)

meta.create_all(engine)