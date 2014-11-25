Shavadoop

My own implementation of MapReduce in Java on a cluster of machines.

Include :
* launch wordcount with parallel distributed computing
* task tracker to relaunch task failed
* manage files transfer between computers, thanks to scp
* manage the number of cores of each computer connected

TODO benchmark


## Hadoop VS Shavadoop <h3>

## Hadoop <h5>

TODO schema
TODO txt

## Shavadoop <h5>

TODO schema
TODO txt for each step
TODO schema thread
TODO txt


## How to use Shavadoop <h3>

* clone the project with git
* edit the config.properties file (included in the src root directory)
* generate the jar file with your favorite IDE (or use the terminal like a boss)
* from the terminal, execute the jar
* have a coffee (or not)
* Enjoy the result. You can use the command : TODO


## To the future <h3>

* change Wordcount implementation to execute generic algorithm
* export the properties file outside the jar (more convenient)
* optimize block's size in the Input Splitting step, with dynamic size (according to the number of mapper workers)
* when the input file is splited into several blocs, it could cut a word in half (serialization with Avro to avoid that ?)
* add JUnit test for regression testing
