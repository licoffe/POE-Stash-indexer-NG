# Introduction

High performance multithreaded indexer for the Path of Exile Stash API coded in C++11.

JSON files are pre-downloaded ahead through a first thread. In a second thread, downloaded files are parsed and data inserted into the database. Insertion leverages the [LOAD DATA INFILE](http://dev.mysql.com/doc/refman/5.7/en/load-data.html) MySQL statement for increased insertion speed.

# How it works

Two threads are used: 

- The first one, *download_loop* downloads JSON changes at a regular interval
- The second one, *processing_loop* parses these files and insert data into the database

As files are downloaded through the first thread, their name is added to a queue. This queue is read by the second thread which progressively parses the corresponding files.

The second thread iterates over stashes, inserting items one by one. Item properties (properties, affixes, requirements and sockets) are however stored for a delayed insertion. When a certain amount of JSON files have been parsed, item properties are written to text files and inserted through 4 different threads (properties, affixes, requirements and sockets).

# Libraries used

JSON files are downloaded with gzip compression using [libCurl](https://curl.haxx.se/libcurl/).

The [RapidJSON](http://rapidjson.org/) library is used to parse JSON files.

Storage is done through MySQL (see schema.sql) and the interface with MySQL is done through the [Connector/C++](https://dev.mysql.com/downloads/connector/cpp/1.1.html) library, the official MySQL C++ communication API.

# Installing required libraries

The first thing you need is to have a running MySQL installation correctly tuned for your hardware.

To run the indexer, you will need to install the required dependencies:

- LibCurl
- RapidJSON
- Connector/C++

## MacOS
On MacOS, download [Mysql-c++-connector](https://dev.mysql.com/downloads/connector/cpp/1.1.html). Extract the archive and copy the content of the `lib` folder to `/usr/local/lib`, and the content of the `include` folder to `/usr/local/include`. Next, download [RapidJSON](http://rapidjson.org/) and copy the content of `include` to `/usr/local/include`.

## Ubuntu
On Ubuntu, install the following packages using `apt-get install libcurl4-gnutls-dev libcurlpp-dev libmysqlcppconn-dev`. You may have to edit your MySQL configuration file (**my.cnf**) to authorize the LOAD DATA INFILE statement. You will also have to download RapidJSON. Next, download [RapidJSON](http://rapidjson.org/) and copy the content of `include` to `/usr/local/include`.

# Compilation and setup

When all dependencies have been installed, open up a terminal and change directory to the indexer one and type `make macos` or `make linux`. If all dependencies have been found, the code should now be compiled. Import the **schema.sql** DB scheme in your MySQL installation. The last step is to edit the **config.cfg** file to put the right credentials in.

When compiled, just run the indexer using `./indexer`.