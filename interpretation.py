from ksql import KSQLAPI
from ksql.errors import KSQLRequestError

ksql_api = KSQLAPI('http://localhost:8088')

try:
    ksql_api.ksql("CREATE STREAM degree_stream (degree INT)
    WITH (KAFKA_TOPIC='temp', VALUE_FORMAT='JSON');")
except KSQLRequestError as e:
    print(e)

try:
ksql_api.ksql("CREATE STREAM processed_stream (name VARCHAR, age INT)
WITH (KAFKA_TOPIC='processed', VALUE_FORMAT='JSON');")
except KSQLRequestError as e:
print(e)

try:
ksql_api.ksql("INSERT INTO processed_stream
SELECT CAST(JSON_EXTRACT(data, '$.name') AS VARCHAR) AS name,
CAST(JSON_EXTRACT(data, '$.age') AS INT) AS age
FROM temp_stream;")
except KSQLRequestError as e:
print(e)

try:
rows = ksql_api.ksql("SELECT * FROM processed_stream EMIT CHANGES;")
for row in rows:
print(row)
except KSQLRequestError as e:
print(e)