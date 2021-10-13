import std/[unittest, tables]

import easy_sqlite3
import easy_sqlite3/[memfs, logfs]

proc select_1(arg: int): tuple[value: int] {.importdb: "SELECT $arg".}

proc create_table() {.importdb: """
  CREATE TABLE mydata(name TEXT PRIMARY KEY NOT NULL, value INT NOT NULL);
""".}

proc insert_data(name: string, value: int) {.importdb: """
  INSERT INTO mydata(name, value) VALUES ($name, $value);
""".}

iterator iterate_data(): tuple[name: string, value: int] {.importdb: """
  SELECT name, value FROM mydata;
""".} = discard

test "simple":
  var db = initDatabase(":memory:")
  check db.select_1(1) == (value: 1)

test "full":
  const dataset = {
    "A": 0,
    "B": 1,
    "C": 2,
    "D": 3,
  }.toTable
  var db = initDatabase("test")
  db.exec "PRAGMA journal_mode=DELETE"
  db.create_table()
  for name, value in dataset:
    db.insert_data name, value
  for name, value in db.iterate_data():
    check name in dataset
    check dataset[name] == value