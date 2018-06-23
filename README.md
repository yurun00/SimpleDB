# SimpleDB

## Basic commands

### 0. Ant basics

Command | Description
--- | ---
ant|Build the default target (for simpledb, this is dist).
ant -projecthelp|List all the targets in `build.xml` with descriptions.
ant dist|Compile the code in src and package it in `dist/simpledb.jar`.
ant test|Compile and run all the unit tests.
ant runtest -Dtest=testname|Run the unit test named `testname`.
ant systemtest|Compile and run all the system tests.
ant runsystest -Dtest=testname|Compile and run the system test named `testname`.

### 1. Unit test

```
$ cd 6.830-lab
$ # run all unit tests
$ ant test
$ # run a specific unit test
$ ant runtest -Dtest=TupleTest
```

### 2. System test

```
$ ant systemtest
$ ant runsystest -Dtest=testname
```

### 3. Creating dummy tables

Create any `.txt` file and convert it to a `.dat` file in SimpleDB's HeapFile format

```
$ java -jar dist/simpledb.jar convert file.txt N
```

where `file.txt` is the name of the file and `N` is the number of columns in the file. Notice that `file.txt` has to be in the following format:

```
int1, int2, ..., intN
int1, int2, ..., intN
int1, int2, ..., intN
int1, int2, ..., intN
```

where each `intN` is a non-negative integer.

To view the content of a table, use the `print` command:

```
$ java -jar dist/simpledb.jar print file.dat N
```

where `file.dat` is the name of a table created with the `convert` command, and `N` is the number of columns in the file.

### Query parser and contest

A query parser for SimpleDB that can use to write and run SQL queries against the database. The first step is to create some data tables and a catalog. Suppose a file `data.txt` with the following contents:

```
1, 10
2, 20
3, 30
4, 40
```

One can convert this into a SimpleDB table using the `convert` command (make sure to type `ant` first)

```
$ java -jar dist/simpledb.jar convert data.txt 2 "int, int"
```

This creates a file `data.dat`. Next, create a catalog file `catalog.txt` with the follow contents:

```
data (f1 int, f2 int)
```

where `data` is the table and the two integer fields are `f1` and `f2`.

Finally, one can invoke the parser by running java from the command line (ant doesn't work properly with interactive targets).

```
$ java -jar dist/simpledb.jar parser catalog.txt
```

One can see the output like

```
$ Added table : data with schema INT(f1), INT(f2),
$ SimpleDB>
```

Finally, one can run query

```
SimpleDB> select d.f1, d.f2 from data d;
Started a new transaction tid = 1221852405823
 ADDING TABLE d(data) TO tableMap
     TABLE HAS tupleDesc INT(d.f1), INT(d.f2),
1 10
2 20
3 30
4 40
5 50
5 50

 6 rows.
----------------
0.16 seconds
SimpleDB>
```

Limitations
* Every field name must be prefaced with its table name, even if the field name is unique. (One can use table name aliases, but not the As keyword)
* Nested queries are supported in the `WHERE` clause, but not the `FROM` clause.
* No arithmetic expressions are supported (for example, you can't take the sum of two fields.)
* At most one `GROUP BY` and one aggregate column are allowed.
* Set-oriented operators like `IN`, `UNION`, and `EXCEPT` are not allowed.
* Only `AND` expressions in the `WHERE` clause are allowed.
* `UPDATE` expressions are not supported.
* The string operator `LIKE` is allowed, but must be written out fully (that is, the Postgres tilde \[~\] shorthand is
not allowed.)
