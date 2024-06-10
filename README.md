## OLAP Toy Database

I am building a OLAP Database from scratch in Rust based on the following [tutorial](https://risinglightdb.github.io/risinglight-tutorial/00-lets-build-a-database.html) (safari or google translate does the trick really well for me) from RisingWave labs. 

The goal of this project is to understand and implement the main components of a database system by doing simple implementations of each. Main components to be developed will be:

* Parser: We are mostly going to rely on sqlparser crate to do sql syntax validation and get the AST.
* Executor
* Catalog
* Planner
* In memory storage