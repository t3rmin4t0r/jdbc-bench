# jdbc-bench
Tool to benchmark HiveServer2 over JDBC

Usage:
java -jar target/jdbc-bench-0.0.1-SNAPSHOT.jar \
-u '<jdbc url>' \
 -t <number of users/threads>\
 -n <number of queries in single iteration of a thread>
 -r <ramp-up time for thread startup> \
 -g <gap time between query runs>
 -qf <query-file>
  
  
  Example 1:
  Running 10 concurrent users on a set of 30 queries 
  Each user/thread is assigned a block of 3 queries to execute. 
  
 java -jar target/jdbc-bench-0.0.1-SNAPSHOT.jar \
-u 'jdbc:hive2://localhost:10007/tpcds_bin_partitioned_orc_10000;transportMode=binary;httpPath=cliservice' \
 -t 10\
 -n 3 \
 -qf queries.txt
  
 Example 2:
 Running 4 concurrent users with ramp up time of 500 ms
 Run all queries in queries32-4.txt once
java -jar target/jdbc-bench-0.0.1-SNAPSHOT.jar \
-u 'jdbc:hive2://localhost:10007/tpcds_bin_partitioned_orc_10000;transportMode=binary;httpPath=cliservice' \
 -t 4\
 -r 500 \
 -qf queries/queries32-4.txt
  
  
  
 
 
 
