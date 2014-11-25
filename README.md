## Shavadoop

My own implementation of MapReduce in Java.

Include :
* launch wordcount with parallel distributed computing on a cluster of machines
* task tracker to relaunch task failed
* manage files transfer between computers, thanks to scp
* manage the number of cores of each computer connected

TODO benchmark


## Hadoop VS Shavadoop

#### Hadoop

![Hadoop MapReduce](res/readme/MapReduceWordCountOverview1.png)

#### Shavadoop

TODO schema


##### Parallel distributed computing

TODO schema thread

TODO txt


## How to use Shavadoop

1. clone the project with git
2. edit the config.properties file (included in the src root directory)
3. generate the jar file with your favorite IDE (or use the terminal like a boss)
4. from the terminal, execute the jar
5. have a coffee (or not)
6. enjoy the result. You can use the command : TODO


## To the future

* change Wordcount implementation to execute generic algorithm
* export the properties file outside the jar (more convenient)
* optimize block's size in the Input Splitting step, with dynamic size (according to the number of mapper workers)
* when the input file is splited into several blocs, it could cut a word in half (serialization with Avro to avoid that ?)
* change the transfer file, currently with scp, by another way (scp has number limited connection)
* add JUnit test for regression testing
