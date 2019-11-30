# mongo-invariants
Generate Daikon dynamic invariants for MongoDB

# Usage
```shell
python3 mongo_to_trace.py <path> <host> <port> <database> <level1> <level2>
```
`path` path to generate files(without file extension)
`host` hostname for your mongoDB server
`port` port for your mongoDB server
`database` database to inspect
`level1` depth of the hierarchy strcuture of collection fields to be flattened
`level2` depth of the hierarchy structure of collection fields to rebuild collections


Example:
```shell
python3 mongo_to_trace.py my_mongodb localhost 27017 test 2 2
```
This generates a `.decls` declaration file and a `.dtrace` dtrace file for all the collections of the database.
To run Daikon on dtrace:
```shell
java -cp $DAIKONDIR/daikon.jar daikon.Daikon my_mongodb.decls my_mongodb.dtrace
```
Please refer to [Daikon user manual](https://plse.cs.washington.edu/daikon/download/doc/) for more information.