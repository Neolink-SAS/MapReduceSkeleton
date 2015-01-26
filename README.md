# MapReduceSkeleton

# Introduction
This is a MapReduce Skeleton job with MariaDB/MySQL (and HBase coming soon) support as input dataset and output.
The SQL configuration is in Main.java, and if you use the UPDATE method, you need to modify the Query in MysqlDBOutputFormat.java  
You need to add 3 parameters to your job: the SQL server URI, the login and the password

# Libs
You need 3 libs:  
* [hadoop-annotations-2.5.0-cdh5.3.0.jar](https://repository.cloudera.com/content/groups/public/org/apache/hadoop/hadoop-annotations/2.5.0-cdh5.3.0/)
* [hadoop-common-2.6.0.jar](http://mirrors.ircam.fr/pub/apache/hadoop/common/hadoop-2.6.0/)
* [hadoop-mapreduce-client-core-2.6.0.jar](http://mirrors.ircam.fr/pub/apache/hadoop/common/hadoop-2.6.0/)

# Licence
This project is licensed under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)