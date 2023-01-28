import easy_sqlite3
import std/[unittest, tables]

proc createTable() {.importdb: "CREATE TABLE data(id INTEGER PRIMARY KEY, value INTEGER)".}
proc insertData(id, value: int): int {.importdb: "INSERT INTO data(id, value) VALUES ($id, $value)".}
proc getData(id: int): tuple[value: int] {.importdb: "SELECT value FROM data WHERE id = $id".}
iterator listData(): tuple[id: int, value: int] {.importdb: "SELECT id, value FROM data".}
  = discard

suite "catch":
  setup:
    var db = initDatabase(":memory:")
    db.createTable()
    check db.insertData(1, 1) == 1
    check db.insertData(2, 4) == 2
  test "insert 1, 2 (should failed)":
    try:
      discard db.insertData(1, 2)
      fail()
    except:
      check getCurrentExceptionMsg() == "UNIQUE constraint failed: data.id"
  test "get data":
    check db.getData(1) == (value: 1)
  test "list data":
    const dataset = {
      1: 1,
      2: 4
    }.toTable
    for id, value in db.listData():
      check dataset[id] == value