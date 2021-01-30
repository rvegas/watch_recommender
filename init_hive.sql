CREATE EXTERNAL TABLE watch_crawls(name STRING, model STRING, price STRING, rating STRING, comments STRING);
LOAD DATA INPATH 'gs://bucket-cluster-ricardo/input/crawlings/creation_watches_copy.csv' INTO TABLE watch_crawls;
CREATE EXTERNAL TABLE watch_amazon(name STRING, model STRING, price STRING, rating STRING, trend STRING);
LOAD DATA INPATH 'gs://bucket-cluster-ricardo/input/datasets/amazon_copy.csv' INTO TABLE watch_amazon;
CREATE EXTERNAL TABLE watch_recommendations(name STRING, model STRING, price STRING, comments STRING, trend STRING) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES('es.resource' = watches, 'es.index.auto.create' = 'true', 'es.nodes' = '35.228.217.66');
INSERT INTO TABLE watch_recommendations SELECT watch_crawls.name, watch_crawls.model, watch_crawls.price, watch_crawls.comments, watch_crawls.rating, watch_amazon.trend FROM watch_crawls LEFT JOIN watch_amazon ON watch_amazon.model = watch_crawls.model;
!quit
