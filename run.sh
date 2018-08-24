# 4 threads with a ramp-up of 500ms
# run all queries in queries32-4.txt once
java -jar target/jdbc-bench-0.0.1-SNAPSHOT.jar \
-u 'jdbc:hive2://localhost:10007/tpcds_bin_partitioned_orc_10000;transportMode=binary;httpPath=cliservice' \
 -t 4\
 -r 500 \
 -qf queries/queries32-4.txt
