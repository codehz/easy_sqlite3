{.used.}

import std/strformat
import ./bindings

type AppendedInfo = object
  name: string
  origvt: ptr SqliteIoMethods

let defaultvfs = sqlite3_vfs_find(nil)

var logvfs = defaultvfs[]
var logvtb: SqliteIoMethods
logvfs.name = "logfs"
logvfs.osfilesize += AppendedInfo.sizeof.cint

proc appended(file: ptr SqliteFile): var AppendedInfo =
  cast[ptr AppendedInfo](cast[ByteAddress](file) + defaultvfs.osfilesize)[]

proc name(file: ptr SqliteFile): string =
  file.appended.name
proc `name=`(file: ptr SqliteFile, value: string) =
  file.appended.name = value

proc origvt(file: ptr SqliteFile): ptr SqliteIoMethods =
  file.appended.origvt
proc `origvt=`(file: ptr SqliteFile, value: ptr SqliteIoMethods) =
  file.appended.origvt = value

{.push warnings:off.}
logvfs.open = proc (vfs: ptr SqliteVFS, name: cstring, file: ptr SqliteFile, flags: OpenFlags, outflags: ptr OpenFlags): ResultCode {.cdecl.} =
  echo fmt"open {name} ({flags})"
  result = defaultvfs.open(defaultvfs, name, file, flags, outflags)
  if result == sr_ok:
    if outflags != nil:
      echo fmt"open-result {outflags[]}"
    file.origvt = file.vtable
    file.vtable = addr logvtb
    file.name = $name
{.pop.}

logvfs.delete = proc (vfs: ptr SqliteVFS, name: cstring, syncDir: bool): ResultCode {.cdecl.} =
  echo fmt"delete {name}"
  defaultvfs.delete(defaultvfs, name, syncDir)

logvtb.version = 2

logvtb.close = proc (file: ptr SqliteFile): ResultCode {.cdecl.} =
  echo fmt"close {file.name}"
  defer: `=destroy`(file.appended)
  file.origvt.close(file)

logvtb.read = proc (file: ptr SqliteFile, buffer: pointer, amt: cint, offset: int64): ResultCode {.cdecl.} =
  echo fmt"read {file.name} (offset: {offset}, size: {amt})"
  file.origvt.read(file, buffer, amt, offset)

logvtb.write = proc (file: ptr SqliteFile, buffer: pointer, amt: cint, offset: int64): ResultCode {.cdecl.} =
  echo fmt"write {file.name} (offset: {offset}, size: {amt})"
  file.origvt.write(file, buffer, amt, offset)

logvtb.truncate = proc (file: ptr SqliteFile, size: cint): ResultCode {.cdecl.} =
  echo fmt"truncate {file.name} {size}"
  file.origvt.truncate(file, size)

logvtb.sync = proc (file: ptr SqliteFile, flags: cint): ResultCode {.cdecl.} =
  echo fmt"sync {file.name} ({flags})"
  file.origvt.sync(file, flags)

logvtb.size = proc (file: ptr SqliteFile, size: var int64): ResultCode {.cdecl.} =
  result = file.origvt.size(file, size)
  echo fmt"size {file.name} = {size}"

logvtb.lock = proc (file: ptr SqliteFile, level: SqliteLockLevel): ResultCode {.cdecl.} =
  echo fmt"lock {file.name} ({level})"
  file.origvt.lock(file, level)

logvtb.unlock = proc (file: ptr SqliteFile, level: SqliteLockLevel): ResultCode {.cdecl.} =
  echo fmt"unlock {file.name} ({level})"
  file.origvt.unlock(file, level)

logvtb.checklock = proc (file: ptr SqliteFile, res: var bool): ResultCode {.cdecl.} =
  result = file.origvt.checklock(file, res)
  echo fmt"checklock {file.name} = {res}"

logvtb.filectl = proc (file: ptr SqliteFile, op: SqliteFileCtlOp, arg: pointer): ResultCode {.cdecl.} =
  echo fmt"filectl {file.name} ({op})"
  case op
  of sf_vfsname:
    let p = sqlite3_mprintf("%s", logvfs.name)
    cast[ptr cstring](arg)[] = p
  of sf_vfs_pointer:
    cast[ptr ptr SqliteVFS](arg)[] = addr logvfs
  else:
    result = file.origvt.filectl(file, op, arg)

logvtb.sectorsize = proc (file: ptr SqliteFile): cint {.cdecl.} =
  result = file.origvt.sectorsize(file)
  echo fmt"sectorsize {file.name} = {result}"

logvtb.device = proc (file: ptr SqliteFile): SqliteDeviceCharacteristics {.cdecl.} =
  result = file.origvt.device(file)
  echo fmt"device {file.name} = {result}"

logvtb.shmmap = proc (file: ptr SqliteFile, pages: cint, pagesize: cint, extend: bool, target: var pointer): ResultCode {.cdecl.} =
  echo fmt"shmmap {file.name} (pages: {pages} pagesize: {pagesize} extend: {extend})"
  file.origvt.shmmap(file, pages, pagesize, extend, target)

logvtb.shmlock = proc (file: ptr SqliteFile, offset: cint, n: cint, flags: SqliteShmLockFlags): ResultCode {.cdecl.} =
  echo fmt"shmlock {file.name} (offset: {offset} n: {n} flags: {flags})"
  file.origvt.shmlock(file, offset, n, flags)

logvtb.shmbarrier = proc (file: ptr SqliteFile) {.cdecl.} =
  echo fmt"shmbarrier {file.name}"
  file.origvt.shmbarrier(file)

logvtb.shmunmap = proc (file: ptr SqliteFile, delete: bool): ResultCode {.cdecl.} =
  echo fmt"shmunmap {file.name} ({delete})"
  file.origvt.shmunmap(file, delete)

check_sqlite sqlite3_vfs_register(logvfs.addr, true)
