# Yet another SQLite wrapper for Nim

Features:

1. Design for ARC/ORC, you don’t need to close the connection manually
2. Use `importdb` macro to create helper function (see examples)
3. Including a memfs implemention， may better than `:memory:` database since it support WAL mode (Experimental, see tests/test_thread)

## Example

Basic usage:

```nim
import std/tables
import easy_sqlite3

# Bind function argument to sql statment
# The tuple return value indicate the query will got exactly 1 result
proc select_1(arg: int): tuple[value: int] {.importdb: "SELECT $arg".}

var db = initDatabase(":memory:")
# Use as a method (the statment will be cached, thats why `var` is required)
echo db.select_1(1).value
# Got 1

# You can bind create statment as well
proc create_table() {.importdb: """
  CREATE TABLE mydata(name TEXT PRIMARY KEY NOT NULL, value INT NOT NULL);
""".}

# Or insert
proc insert_data(name: string, value: int) {.importdb: """
  INSERT INTO mydata(name, value) VALUES ($name, $value);
""".}

# And you can create iterator by the same way (the `= discard` is required, since iterator must have body in nim)
iterator iterate_data(): tuple[name: string, value: int] {.importdb: """
  SELECT name, value FROM mydata;
""".} = discard

const dataset = {
  "A": 0,
  "B": 1,
  "C": 2,
  "D": 3,
}.toTable
db.create_table()
# Use transaction (commit by default)
db.transaction:
  for name, value in dataset:
    db.insert_data name, value
  commit() # optional
  # Never goes here

for name, value in db.iterate_data():
  assert name in dataset
  assert dataset[name] == value
```
