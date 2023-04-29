import std/unittest
import std/options

import easy_sqlite3

proc returnOptionNone(): tuple[col: Option[int]] {.importdb: "SELECT NULL;".}
proc returnOptionSome(): tuple[col: Option[int]] {.importdb: "SELECT 1;".}

proc takeOption(col: Option[int]): tuple[val: int, is_null: bool] {.importdb: "SELECT COALESCE($col, 0), $col IS NULL".}

iterator options(a: Option[int], b: Option[int], c: Option[int]): tuple[val: Option[int]] {.importdb: "VALUES ($a+1), ($b+1), ($c+1)".} = discard

suite "option":
  setup:
    var db = initDatabase(":memory:")

  test "return option none":
    let col = db.returnOptionNone().col
    check col.is_none()

  test "return option some":
    let col = db.returnOptionSome().col
    check col.is_some()
    check col.get == 1

  test "take option none":
    let res = db.takeOption(none(int))
    check res.is_null
    check res.val == 0

  test "take option some":
    let res = db.takeOption(some(1))
    check not res.is_null
    check res.val == 1

  test "iterator options":
    var res: seq[Option[int]] = @[]
    for row in db.options(some(1), none(int), some(2)):
      res.add(row.val)
    check res == @[some(2), none(int), some(3)]
