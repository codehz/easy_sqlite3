import std/[tables, random, os, times, strformat]

import easy_sqlite3
import easy_sqlite3/memfs

const useMemFs = not defined(defaultMemoryDB)

when useMemFs:
  template retry(body: untyped) = body
else:
  enableSharedCache()
  var failedCount = 0
  template retry(body: untyped) =
    var failed = 0
    while true:
      try:
        body
        break
      except:
        failed.inc
    if failed > 0:
      failedCount.atomicInc(failed)

proc create_table() {.importdb: """
  CREATE TABLE store(key INTEGER PRIMARY KEY, value INT NOT NULL);
""".}

proc insert_data(value: int) {.importdb: """
  INSERT INTO store(value) VALUES ($value)
""".}

proc count_items(): tuple[count: int] {.importdb: "SELECT count(*) FROM store".}

proc connectDatabase(): Database =
  when useMemFs:
    initDatabase("file:memdb1", {so_readwrite, so_create, so_uri})
  else:
    initDatabase("file:memdb1?mode=memory&cache=shared", {so_readwrite, so_create, so_uri})
  

var gdb = connectDatabase()
gdb.create_table()
gdb.exec "VACUUM"

const COUNT = 1000000
const GROUP = 100

proc worker_fn() {.thread.} =
  echo "thread start"
  var tdb = connectDatabase()
  var r = initRand(42)
  for _ in 0..<(COUNT div GROUP):
    retry:
      tdb.transactionImmediate:
        for _ in 0..<GROUP:
          let val = r.rand(1048576)
          # increase the chance of collision
          if val < 1024:
            sleep(1)
          tdb.insert_data(val)
        retry:
          commit()

var worker: Thread[void]
createThread(worker, worker_fn)

let init = cpuTime()
var prev = init
while true:
  var c: int
  retry:
    c = gdb.count_items().count
  let curr = cpuTime()
  let diff = curr - prev - 0.2
  if diff > 0:
    when useMemFs:
      echo fmt"{curr - init:>6.1f}s: {c:>7}"
    else:
      echo fmt"{curr - init:>6.1f}s: {c:>7} failures: {failedCount}"
    prev = curr - diff
  if c == COUNT:
    break

when useMemFs:
  echo fmt"time: {cpuTime() - init:>9.4f}s"
else:
  echo fmt"time: {cpuTime() - init:>9.4f}s failures: {failedCount}"

worker.joinThread()