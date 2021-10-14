{.used.}

import std/[tables, locks]
import ./bindings

var glock: Lock
glock.initLock()

const
  PAGE_SIZE = 4096
  SHM_PAGE_SIZE = 32768

type
  FileKind* = enum
    fk_main
    fk_temp
    fk_transient
    fk_main_journal
    fk_temp_journal
    fk_sub_journal
    fk_super_journal
    fk_wal
  ShmLockKind = enum
    slk_none,
    slk_shared,
    slk_exclusive
  ShmLockState = distinct int
  ShmFile = object
    chunks: seq[ref array[SHM_PAGE_SIZE, byte]]
    shmlocks: array[8, ShmLockState]
    lock: Lock
  MemoryFile = object
    kind: FileKind
    pages: seq[ref array[PAGE_SIZE, byte]]
    shm: ref ShmFile
    size: int
    refc: int
    fileLock: Lock
    state: SqliteLockLevel
    shareds: int
  MemoryFileStat* = object
    name*: string
    kind*: FileKind
    size*: int
    refc*: int
  MemoryFileInfo = object
    base: SqliteFile
    data: ptr MemoryFile
    locklevel: SqliteLockLevel
    shmlocklevel: array[8, ShmLockKind]

proc createMemoryFile(kind: FileKind): ptr MemoryFile =
  result = create MemoryFile
  result[].kind = kind
  result[].fileLock.initLock()
  result[].pages = newSeqOfCap[ref array[PAGE_SIZE, byte]](1024)

proc initShmFile(file: ptr MemoryFile) {.inline.} =
  if file.shm == nil:
    new file.shm
    file.shm.lock.initLock()

proc shm_try_share(state: ShmLockState): bool = state.int >= 0
proc shm_try_unshare(state: ShmLockState): bool = state.int > 0
proc shm_try_exclusive(state: ShmLockState): bool = state.int == 0
proc shm_try_release(state: ShmLockState): bool = state.int == -1

proc shm_share(state: var ShmLockState) = state.int.inc
proc shm_unshare(state: var ShmLockState) = state.int.dec
proc shm_exclusive(state: var ShmLockState) = state = (-1).ShmLockState
proc shm_release(state: var ShmLockState) = state = 0.ShmLockState

proc lockShmFile(file: ptr MemoryFile, offset, length: int, flags: SqliteShmLockFlags): bool =
  assert offset >= 0 and length > 0 and offset + length <= 8
  assert file.shm != nil
  let (tfn, afn) = if shm_unlock in flags:
    if shm_shared in flags:
      (shm_try_unshare, shm_unshare)
    else:
      (shm_try_release, shm_release)
  else:
    if shm_shared in flags:
      (shm_try_share, shm_share)
    else:
      (shm_try_exclusive, shm_exclusive)
  file.shm.lock.withLock:
    for i in offset..<offset + length:
      if not tfn(file.shm.shmlocks[i]):
        return false
    for i in offset..<offset + length:
      afn(file.shm.shmlocks[i])
  return true

proc data(file: ptr SqliteFile): ptr MemoryFile = cast[ptr MemoryFileInfo](file)[].data
proc `data=`(file: ptr SqliteFile, ret: ptr MemoryFile) =
  cast[ptr MemoryFileInfo](file).data = ret

proc locklevel(file: ptr SqliteFile): SqliteLockLevel = cast[ptr MemoryFileInfo](file)[].locklevel
proc `locklevel=`(file: ptr SqliteFile, ret: SqliteLockLevel) =
  cast[ptr MemoryFileInfo](file).locklevel = ret

proc openMemoryFile(target: ptr SqliteFile, mf: ptr MemoryFile) =
  target.data = mf

proc close(self: ptr MemoryFile) =
  self.refc.atomicDec

proc resize(self: ptr MemoryFile, size: int) =
  if self.size == size: return
  let
    pagecount    = ((size - 1) div PAGE_SIZE) + 1
    oldpagecount = self.pages.len
  self.pages.setLen(pagecount)
  if pagecount > oldpagecount:
    for i in oldpagecount..<pagecount:
      new self.pages[i]
  self.size = size

iterator range(self: ptr MemoryFile; offset, length: int): tuple[idx: int, value: ptr byte] =
  var
    cursor  = 0
    pageidx = offset div PAGE_SIZE
    pageoff = offset mod PAGE_SIZE
  template page(): untyped = self.pages[pageidx]
  for _ in offset..<offset + length:
    yield (idx: cursor, value: addr page[pageoff])
    cursor.inc
    pageoff.inc
    if pageoff == PAGE_SIZE:
      pageoff = 0
      pageidx.inc

proc readBuffer(self: ptr MemoryFile, buffer: ptr UncheckedArray[byte], length: int, offset: int): ResultCode =
  let
    max   = length + offset
    bound = min(self.size, max)
  for i, p in self.range(offset, bound - offset):
    buffer[i] = p[]
  let remain = min(max - bound, length)
  if remain > 0:
    result = sr_ioerr_short_read
    zeroMem(addr buffer[length - remain], remain)

proc writeBuffer(self: ptr MemoryFile, buffer: ptr UncheckedArray[byte], length: int, offset: int): ResultCode =
  let max = length + offset
  if max > self.size:
    self.resize(max)
  for i, p in self.range(offset, length):
    p[] = buffer[i]

var root = initTable[string, ptr MemoryFile]()

iterator listMemfs*(): MemoryFileStat =
  glock.withLock:
    for name, file in root:
      yield MemoryFileStat(name: name, kind: file.kind, size: file.size, refc: file.refc)

proc removeMemoryFile*(name: string): bool =
  glock.withLock:
    root.withValue(name, file) do:
      if file.refc != 0:
        return false
      `=destroy` file[]
      dealloc file
      root.del name
  return true

proc getMemoryFile(cname: cstring, filekind: FileKind): ptr MemoryFile =
  let name = $cname
  glock.withLock:
    root.withValue(name, file) do:
      assert file.kind == filekind
      result = file[]
    do:
      result = createMemoryFile(filekind)
      root[name] = result
  result.refc.inc

let defaultvfs = sqlite3_vfs_find(nil)
var memvfs = defaultvfs[]
var memios: SqliteIoMethods
memvfs.version = 2
memvfs.name = "memvfs"
memvfs.maxpathname = 255
memvfs.osfilesize = MemoryFileInfo.sizeof.cint
memios.version = 2

memvfs.open = proc (vfs: ptr SqliteVFS, name: cstring, file: ptr SqliteFile, flags: OpenFlags, outflags: ptr OpenFlags): ResultCode {.cdecl.} =
  var kind: FileKind
  if so_main_db in flags:
    kind = fk_main
  elif so_temp_db in flags:
    kind = fk_temp
  elif so_transient_db in flags:
    kind = fk_transient
  elif so_main_journal in flags:
    kind = fk_main_journal
  elif so_temp_journal in flags:
    kind = fk_temp_journal
  elif so_subjournal in flags:
    kind = fk_sub_journal
  elif so_super_journal in flags:
    kind = fk_super_journal
  elif so_wal in flags:
    kind = fk_wal
  else:
    return sr_misuse
  file.openMemoryFile getMemoryFile(name, kind)
  file.vtable = addr memios
  if outflags != nil:
    if so_readwrite in flags:
      outflags[] = {so_readwrite}
    else:
      outflags[] = {so_readonly}

memvfs.delete = proc (vfs: ptr SqliteVFS, cname: cstring, syncDir: bool): ResultCode {.cdecl.} =
  let name = $cname
  glock.withLock:
    root.withValue(name, file):
      assert file.refc >= 0
      if file.refc == 0:
        `=destroy` file[]
        dealloc file
        root.del name
      else:
        return sr_ioerr

memvfs.access = proc (vfs: ptr SqliteVFS, cname: cstring, flag: SqliteAccessFlag, res: var bool): ResultCode {.cdecl.} =
  let name = $cname
  glock.withLock:
    res = name in root

memvfs.fullpathname = proc (vfs: ptr SqliteVFS, name: cstring, nOut: cint, zOut: cstring): ResultCode {.cdecl.} =
  let l = name.len + 1
  if l >= nOut:
    return sr_cantopen
  copyMem(zOut, name, l)

memios.close = proc (file: ptr SqliteFile): ResultCode {.cdecl.} =
  file.data.close()

memios.read = proc (file: ptr SqliteFile, buffer: pointer, amt: cint, offset: int64): ResultCode {.cdecl.} =
  result = file.data.readBuffer(cast[ptr UncheckedArray[byte]](buffer), amt.int, offset.int)

memios.write = proc (file: ptr SqliteFile, buffer: pointer, amt: cint, offset: int64): ResultCode {.cdecl.} =
  result = file.data.writeBuffer(cast[ptr UncheckedArray[byte]](buffer), amt.int, offset.int)

memios.truncate = proc (file: ptr SqliteFile, size: cint): ResultCode {.cdecl.} =
  file.data.resize(size.int)

memios.sync = proc (file: ptr SqliteFile, flags: cint): ResultCode {.cdecl.} = fence()

memios.size = proc (file: ptr SqliteFile, size: var int64): ResultCode {.cdecl.} =
  size = file.data.size.int64

memios.lock = proc (file: ptr SqliteFile, level: SqliteLockLevel): ResultCode {.cdecl.} =
  if level <= file.locklevel:
    return
  file.data.fileLock.withLock:
    assert file.data[].state >= file.locklevel
    case file.locklevel:
    of sl_none: # from
      case level:
      of sl_shared: # to
        case file.data[].state:
        of sl_none: # global
          file.data[].shareds = 1
        of sl_shared, sl_reserved: # global
          file.data[].shareds.inc
        else:
          return sr_busy
      else:
        return sr_misuse
    of sl_shared: # from
      case level:
      of sl_reserved: # to
        case file.data[].state:
        of sl_shared: # global
          discard
        else:
          return sr_busy
      of sl_pending: # to
        case file.data[].state:
        of sl_shared: # global
          discard
        else:
          return sr_busy
      of sl_exclusive: # to
        case file.data[].state:
        of sl_shared: # global
          if file.data[].shareds > 1:
            return sr_busy
          file.data[].shareds = 0
        else:
          return sr_busy
      else:
        return sr_misuse
    of sl_reserved: # from
      case level:
      of sl_pending: # to
        case file.data[].state:
        of sl_reserved: # global
          discard
        else:
          return sr_busy
      of sl_exclusive: # to
        case file.data[].state:
        of sl_reserved: # global
          if file.data[].shareds > 1:
            return sr_busy
          file.data[].shareds = 0
        else:
          return sr_busy
      else:
        return sr_misuse
    of sl_pending: # from
      case level:
      of sl_exclusive: # to
        if file.data[].shareds > 1:
          return sr_busy
        file.data[].shareds = 0
      else:
        return sr_misuse
    else:
      assert false, "invalid lock state"
      return sr_error
    file.locklevel = level
    file.data[].state = level
  
memios.unlock = proc (file: ptr SqliteFile, level: SqliteLockLevel): ResultCode {.cdecl.} =
  if level >= file.locklevel:
    return
  if not (level in {sl_none, sl_shared}):
    return sr_misuse
  file.data.fileLock.withLock:
    assert file.data[].state >= file.locklevel
    case file.locklevel:
    of sl_exclusive: # from
      assert file.data[].shareds == 0
      if level == sl_shared:
        file.data[].shareds = 1
      file.data[].state = level
    of sl_pending, sl_reserved: # from
      assert file.data[].state == file.locklevel
      file.data[].state = sl_shared
      if level == sl_none:
        file.data[].shareds.dec
        if file.data[].shareds == 0:
          file.data[].state = sl_none
    of sl_shared: # from
      assert level == sl_none
      assert file.data[].state >= sl_shared
      file.data[].shareds.dec
      if file.data[].shareds == 0:
        assert file.data[].state == sl_shared
        file.data[].state = sl_none
    else:
      assert false, "invalid unlock state"
      return sr_error
    file.locklevel = level

memios.checklock = proc (file: ptr SqliteFile, outres: var bool): ResultCode {.cdecl.} =
  file.data.fileLock.withLock:
    outres = file.data[].state >= sl_reserved

memios.filectl = proc (file: ptr SqliteFile, op: SqliteFileCtlOp, arg: pointer): ResultCode {.cdecl.} =
  case op
  of sf_vfsname:
    let p = sqlite3_mprintf("%s", memvfs.name)
    cast[ptr cstring](arg)[] = p
  of sf_vfs_pointer:
    cast[ptr ptr SqliteVFS](arg)[] = addr memvfs
  else:
    return sr_notfound

memios.sectorsize = proc (file: ptr SqliteFile): cint {.cdecl.} =
  return PAGE_SIZE

memios.device = proc (file: ptr SqliteFile): SqliteDeviceCharacteristics {.cdecl.} =
  return { dev_sequential, dev_atomic4k, dev_safe_append, dev_powersafe_overwrite }

memios.shmmap = proc (file: ptr SqliteFile, page: cint, pagesize: cint, extend: bool, target: var pointer): ResultCode {.cdecl.} =
  assert file.data.kind in {fk_main, fk_temp}
  assert pagesize <= SHM_PAGE_SIZE
  file.data.initShmFile()
  if page >= file.data.shm.chunks.len:
    if extend:
      let oldlen = file.data.shm.chunks.len
      file.data.shm.chunks.setLen(page + 1)
      for i in oldLen..page:
        new file.data.shm.chunks[i]
    else:
      target = nil
      return
  target = file.data.shm.chunks[page][0].addr

memios.shmlock = proc (file: ptr SqliteFile, offset: cint, n: cint, flags: SqliteShmLockFlags): ResultCode {.cdecl.} =
  if not lockShmFile(file.data, offset.int, n.int, flags):
    return sr_busy

memios.shmbarrier = proc (file: ptr SqliteFile) {.cdecl.} = fence()

memios.shmunmap = proc (file: ptr SqliteFile, delete: bool): ResultCode {.cdecl.} = discard

check_sqlite sqlite3_vfs_register(addr memvfs, true)