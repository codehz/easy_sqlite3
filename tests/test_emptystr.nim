import easy_sqlite3
import std/[unittest, tables]

proc getEmptyString(): tuple[value: string] {.importdb: "SELECT ''".}

test "get empty string":
  var db = initDatabase(":memory:")
  check db.getEmptyString() == (value: "")