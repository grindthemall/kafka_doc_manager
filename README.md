#**Kafka Document Manager for mongo-connector**

This Python 2.7 program runs against MongoDB, and extracts various MongoDB collections and imports those collections into Kafka topics
 wrote this to facilitate pushing data into Kafka so that we can processes it in multiple places without duplicating the data.

This document manager does NOT use batch sizes, but instead relies upon the Kafka plugin to block as needed to get data into Kafka.  This might have a detrimental effect on the speed of the plugin.

This has been tested with regular MongoDB, as well as sharded MongoDB (mongos).

This doc manager requires the installation and use of the mongo-connector found here:
(https://github.com/mongodb-labs/mongo-connector)

Given I did not have time to write a proper setup.py the user MUST install via pip the following packages:

- kafka-python
- bson
- bson.json_util (the mongo-connector package may install the last two)

Once those packages are installed, copy the kafka_doc_manager.py program into:
```
/usr/lib/python2.7/site-packages/mongo_connector/doc_managers/
```

To use this document manager create a command line similar to the following changing the Database and Collection names to match yours.  **DB Names and Collection names are case sensitive**:
```
mongo-connector -m <your_mongo_db_ip_here>:27017 -t <your_kafka_ip_here>:9092 -d kafka_doc_manager --continue-on-error -n <dbname>.<collection> -g <collection>.<dbname>
```
You may specify more than one db.collection by separating with commas like so:
```
mongo-connector -m <your_mongo_db_ip_here>:27017 -t <your_kafka_ip_here>:9092 -d kafka_doc_manager --continue-on-error -n <dbname>.<collection>,<dbname2>.<collection2> -g <collection>.<dbname>,<dbname2>.<collection2>
```

Currently the way this doc manager is written, it is better to run mongo-connector once for each topic/db.collection from its own "topic" directory.  Running multiple instances from the same start directory seems to cause confusion in accessing the oplog tracking files.

*One warning about fields that are timestamp fields in MongoDB.  This program uses a BSON to JSON converter from MongoDB that indents JSON by one level when converting time fields.  So instead of:*
```
{
 "ts": timestamp
}
```
*You will get:*
```
{
 "ts":
      {
       "$date": timestamp
      }
}
```

Feel free to contact me on Git should you run into any errors (there might be a few as I don't consider myself a real developer, more of "hack it together" type).   There is no license to use the code I have published.

A big thanks to the folks at @mongodb-labs/mongo-connector for the great work they did in creating the mongo-connector.
