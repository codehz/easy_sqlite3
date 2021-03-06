import std/[unittest, tables]

import easy_sqlite3
import easy_sqlite3/[memfs, logfs]

proc select_1(arg: int): tuple[value: int] {.importdb: "SELECT $arg".}
proc select_2(arg1, arg2: int): tuple[value: int, value2: int] {.importdb: "SELECT $arg1, $arg2".}

proc insert_data(name: string, value: int) {.importdb: """
  INSERT INTO mydata(name, value) VALUES ($name, $value);
""".}

iterator iterate_data(): tuple[name: string, value: int] {.importdb: """
  SELECT name, value FROM mydata;
""".} = discard

proc count_data(): tuple[count: int] {.importdb: "SELECT count(*) FROM mydata".}

test "simple":
  var db = initDatabase(":memory:")
  check db.select_1(1) == (value: 1)

test "multiple":
  var db = initDatabase(":memory:")
  check db.select_2(1, 2) == (value: 1, value2: 2)

test "full":
  const dataset = {
    "A": 0,
    "B": 1,
    "C": 2,
    "D": 3,
  }.toTable
  var db = initDatabase("test")
  db.execM(
    "PRAGMA journal_mode=DELETE",
    "CREATE TABLE mydata(name TEXT PRIMARY KEY NOT NULL, value INT NOT NULL) WITHOUT ROWID;"
  )
  db.transaction:
    for name, value in dataset:
      db.insert_data name, value
  db.exec "VACUUM"
  for name, value in db.iterate_data():
    check name in dataset
    check dataset[name] == value
  check db.count_data() == (count: dataset.len)