#!/usr/bin/env bash

HOST="root@localhost"
FILE="/srv/train.csv"

if ssh $HOST -p 2222 "test -e $FILE"; then
    echo "file is already uploaded to virtual box"
else
    echo "uploading csv file virtual box"
    scp -P 2222 ./output_data/train.csv  $HOST:/srv
    echo "uploading csv to hdfs"
    ssh -t $HOST -p 2222 'sudo hdfs dfs -put /srv/train.csv  /input_data/Pilip_Varabyou/train/'
fi

# ssh to beeline cli
ssh $HOST -p 2222 << EOF
beeline -u jdbc:hive2://localhost:10000 -n hive;

# Create external table based on uploaded train file
CREATE EXTERNAL TABLE IF NOT EXISTS hive(id INT, date_time STRING, site_name INT, posa_continent INT, user_location_country INT,
                                               user_location_region INT, user_location_city INT, orig_destination_distance DOUBLE, user_id INT,
                                               is_mobile TINYINT, is_package INT, channel INT, srch_ci STRING, srch_co STRING,
                                               srch_adults_cnt INT, srch_children_cnt INT, srch_rm_cnt INT, srch_destination_id INT,
                                               srch_destination_type_id INT, hotel_continent INT, hotel_country INT, hotel_market INT,
                                               is_booking TINYINT, cnt BIGINT, hotel_cluster INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input_data/Pilip_Varabyou/train/';

# Write hive script to calculate the longest period of stay of couples with children
SELECT *
FROM hive as h
WHERE datediff(srch_co, srch_ci) IN (
  	SELECT MAX (datediff(srch_co, srch_ci) )
	FROM hive
	WHERE srch_adults_cnt = 2
	AND srch_children_cnt > 0 )
EOF