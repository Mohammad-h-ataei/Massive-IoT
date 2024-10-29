from ksql import KSQLAPI
from ksql.errors import KSQLRequestError

# Create a KSQLAPI instance to interact with the ksqlDB server
ksql_api = KSQLAPI('http://localhost:8088')

# Create the degree_stream to receive the input data
try:
    ksql_api.ksql("CREATE STREAM degree_stream (degree INT) \
                   WITH (KAFKA_TOPIC='degree_topic', VALUE_FORMAT='AVRO');")
except KSQLRequestError as e:
    print(e)

# Create the degree_mapping table to hold the mapping of the degree range and its corresponding characteristic
try:
    ksql_api.ksql("CREATE TABLE degree_mapping (range VARCHAR PRIMARY KEY, characteristic VARCHAR) \
                   WITH (KAFKA_TOPIC='degree_mapping_topic', VALUE_FORMAT='AVRO');")
except KSQLRequestError as e:
    print(e)

# Insert the mapping values into the degree_mapping table
try:
    ksql_api.ksql("INSERT INTO degree_mapping (range, characteristic) VALUES ('0<X<10', 'need attention');")
    ksql_api.ksql("INSERT INTO degree_mapping (range, characteristic) VALUES ('10<X<15', 'good');")
    ksql_api.ksql("INSERT INTO degree_mapping (range, characteristic) VALUES ('15<X<30', 'perfect');")
    ksql_api.ksql("INSERT INTO degree_mapping (range, characteristic) VALUES ('X<0', 'dangerous');")
except KSQLRequestError as e:
    print(e)

# Create the processed_degree_stream by joining the degree_stream and degree_mapping table on the degree range
try:
    ksql_api.ksql("CREATE STREAM processed_degree_stream AS \
                   SELECT degree, characteristic \
                   FROM degree_stream \
                   JOIN degree_mapping \
                   ON degree_mapping.range LIKE CONCAT('%', CAST(degree AS VARCHAR), '%');")
except KSQLRequestError as e:
    print(e)

# Create the output_stream to output the processed degree with its corresponding characteristic
try:
    ksql_api.ksql("CREATE STREAM output_stream (degree INT, characteristic VARCHAR) \
                   WITH (KAFKA_TOPIC='output_topic', VALUE_FORMAT='AVRO');")
except KSQLRequestError as e:
    print(e)

# Insert the processed data into the output_stream
try:
    ksql_api.ksql("INSERT INTO output_stream SELECT * FROM processed_degree_stream;")
except KSQLRequestError as e:
    print(e)
