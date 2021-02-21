# Invisible-Loading


Goal of this project is to describe Invisible Loading: a scheme that achieves the low time-to-first analysis of MapReduce jobs over a distributed file system while still yielding the long-term performance benefits of database systems.

Invisible Loading gives the illusion that user code is operating directly on the file system, but every time a file is accessed, progress is made towards loading the data into a database system, and then incrementally clustering and indexing the data.

