import std/[tables, random, os, times, strformat]

import easy_sqlite3
import easy_sqlite3/memfs

const useMemFs = true

when not useMemFs:
  enableSharedCache()
  var failedCount = 0

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

const COUNT = 100000

proc worker_fn() {.thread.} =
  echo "thread start"
  var tdb = connectDatabase()
  var r = initRand(42)
  for _ in 0..<COUNT:
    let val = r.rand(1048576)
    # increase the chance of collision
    if val > 1024:
      sleep(1)
    when useMemFs:
      tdb.insert_data(val)
    else:
      var failed = 0
      block retry:
        while true:
          try:
            tdb.insert_data(val)
            break retry
          except:
            failed.inc
      if failed > 0:
        failedCount.atomicInc(failed)

var worker: Thread[void]
createThread(worker, worker_fn)

let init = cpuTime()
var prev = init
while true:
  let c = gdb.count_items().count
  let curr = cpuTime()
  let diff = curr - prev - 0.2
  if diff > 0:
    when useMemFs:
      echo fmt"{curr - init:>6.1f}s: {c:<6}"
    else:
      echo fmt"{curr - init:>6.1f}s: {c:<6} failures: {failedCount}"
    prev = curr - diff
  if c == COUNT:
    break

when useMemFs:
  echo fmt"time: {cpuTime() - init:>9.5f}s"
else:
  echo fmt"time: {cpuTime() - init:>9.5f}s failures: {failedCount}"

worker.joinThread()