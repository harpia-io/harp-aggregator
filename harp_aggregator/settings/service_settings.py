import os

# DB Env
DBUSER = os.getenv('DBUSER', 'harpia')
DBPASS = os.getenv('DBPASS', 'harpia')
DBHOST = os.getenv('DBHOST', '127.0.0.1')
DBPORT = os.getenv('DBPORT', '3306')
DBSCHEMA = os.getenv('DBSCHEMA', 'harp_dev')

# Flask Env
FLASK_SERVER_PORT = os.getenv('FLASK_SERVER_PORT', 8081)
SERVICE_NAMESPACE = os.getenv('SERVICE_NAMESPACE', 'dev')
ENVIRONMENTS_HOST = os.getenv('ENVIRONMENTS_HOST', 'dev.harpia.io/harp-environments')

AEROSPIKE_HOST = os.getenv('AEROSPIKE_HOST', '127.0.0.1')
AEROSPIKE_PORT = int(os.getenv('AEROSPIKE_PORT', 3000))
AEROSPIKE_NAMESPACE = os.getenv('AEROSPIKE_NAMESPACE', 'harpia')

UPDATE_AEROSPIKE_SECONDS = int(os.getenv('UPDATE_AEROSPIKE_SECONDS', 10))
UPDATE_ENVIRONMENTS_SECONDS = int(os.getenv('UPDATE_ENVIRONMENTS_SECONDS', 60))

