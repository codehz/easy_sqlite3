import std/[tables, options, hashes]
import ./utils
import std/macros

when not defined(bundled_sqlite3):
  when defined(windows):
    when defined(nimOldDlls):
      const sqlite3dll = "sqlite3.dll"
    elif defined(cpu64):
      const sqlite3dll = "sqlite3_64.dll"
    else:
      const sqlite3dll = "sqlite3_32.dll"
  elif defined(macosx):
    const sqlite3dll = "libsqlite3(|.0).dylib"
  else:
    const sqlite3dll = "libsqlite3.so(|.0)"
  macro sqlite3linkage(f: untyped) =
    if f[4].kind == nnkEmpty:
      f[4] = newTree nnkPragma
    f[4].add newColonExpr(ident "dynlib", newLit sqlite3dll)
    f[4].add ident "importc"
    f
else:
  {.compile(
    "../../bundled/sqlite3.c",
    """
    -DSQLITE_ENABLE_FTS5=1
    -DSQLITE_ENABLE_RTREE=1
    -DSQLITE_ENABLE_GEOPOLY=1
    -DSQLITE_ENABLE_DBSTAT_VTAB=1
    -DSQLITE_ENABLE_JSON1=1
    -DSQLITE_ENABLE_RBU=1
    -DSQLITE_OMIT_DEPRECATED=1
    -DSQLITE_ENABLE_MATH_FUNCTIONS=1
    -DSQLITE_DQS=0
    """
  ).}
  macro sqlite3linkage(f: untyped) =
    if f[4].kind == nnkEmpty:
      f[4] = newTree nnkPragma
    f[4].add ident "importc"
    f

type RawDatabase* = object
type RawStatement* = object
type RawValue* = object

type CachedHash[T] = object
  cache: int
  value: T

func `==`[T](a, b: CachedHash[T]): bool = a.value == b.value
func hash[T](self: CachedHash[T]): int {.inline.} = self.cache
converter cacheHash[T](original: T): CachedHash[T] =
  CachedHash[T](cache: original.hash, value: original)
func compileTimeHash[T](original: static[T]): CachedHash[T] =
  CachedHash[T](cache: original.hash, value: original)

type Statement* = object
  raw*: ptr RawStatement

type Database* = object
  raw*: ptr RawDatabase
  stmtcache: Table[CachedHash[string], ref Statement]

type ResultCode* {.pure.} = enum
  sr_ok                      = 0,
  sr_error                   = 1,
  sr_internal                = 2,
  sr_perm                    = 3,
  sr_abort                   = 4,
  sr_busy                    = 5,
  sr_locked                  = 6,
  sr_nomem                   = 7,
  sr_readonly                = 8,
  sr_interrupt               = 9,
  sr_ioerr                   = 10,
  sr_corrupt                 = 11,
  sr_notfound                = 12,
  sr_full                    = 13,
  sr_cantopen                = 14,
  sr_protocol                = 15,
  sr_empty                   = 16,
  sr_schema                  = 17,
  sr_toobig                  = 18,
  sr_constraint              = 19,
  sr_mismatch                = 20,
  sr_misuse                  = 21,
  sr_nolfs                   = 22,
  sr_auth                    = 23,
  sr_format                  = 24,
  sr_range                   = 25,
  sr_notadb                  = 26,
  sr_notice                  = 27,
  sr_warning                 = 28,
  sr_row                     = 100,
  sr_done                    = 101,
  sr_ok_load_permanently     = 256,
  sr_error_missing_collseq   = 257,
  sr_busy_recovery           = 261,
  sr_locked_sharedcache      = 262,
  sr_readonly_recovery       = 264,
  sr_ioerr_read              = 266,
  sr_corrupt_vtab            = 267,
  sr_cantopen_notempdir      = 270,
  sr_constraint_check        = 275,
  sr_notice_recover_wal      = 283,
  sr_warning_autoindex       = 284,
  sr_error_retry             = 513,
  sr_abort_rollback          = 516,
  sr_busy_snapshot           = 517,
  sr_locked_vtab             = 518,
  sr_readonly_cantlock       = 520,
  sr_ioerr_short_read        = 522,
  sr_corrupt_sequence        = 523,
  sr_cantopen_isdir          = 526,
  sr_constraint_commithook   = 531,
  sr_notice_recover_rollback = 539,
  sr_error_snapshot          = 769,
  sr_busy_timeout            = 773,
  sr_readonly_rollback       = 776,
  sr_ioerr_write             = 778,
  sr_corrupt_index           = 779,
  sr_cantopen_fullpath       = 782,
  sr_constraint_foreignkey   = 787,
  sr_readonly_dbmoved        = 1032,
  sr_ioerr_fsync             = 1034,
  sr_cantopen_convpath       = 1038,
  sr_constraint_function     = 1043,
  sr_readonly_cantinit       = 1288,
  sr_ioerr_dir_fsync         = 1290,
  sr_cantopen_dirtywal       = 1294,
  sr_constraint_notnull      = 1299,
  sr_readonly_directory      = 1544,
  sr_ioerr_truncate          = 1546,
  sr_cantopen_symlink        = 1550,
  sr_constraint_primarykey   = 1555,
  sr_ioerr_fstat             = 1802,
  sr_constraint_trigger      = 1811,
  sr_ioerr_unlock            = 2058,
  sr_constraint_unique       = 2067,
  sr_ioerr_rdlock            = 2314,
  sr_constraint_vtab         = 2323,
  sr_ioerr_delete            = 2570,
  sr_constraint_rowid        = 2579,
  sr_ioerr_blocked           = 2826,
  sr_constraint_pinned       = 2835,
  sr_ioerr_nomem             = 3082,
  sr_ioerr_access            = 3338,
  sr_ioerr_checkreservedlock = 3594,
  sr_ioerr_lock              = 3850,
  sr_ioerr_close             = 4106,
  sr_ioerr_dir_close         = 4362,
  sr_ioerr_shmopen           = 4618,
  sr_ioerr_shmsize           = 4874,
  sr_ioerr_shmlock           = 5130,
  sr_ioerr_shmmap            = 5386,
  sr_ioerr_seek              = 5642,
  sr_ioerr_delete_noent      = 5898,
  sr_ioerr_mmap              = 6154,
  sr_ioerr_gettemppath       = 6410,
  sr_ioerr_convpath          = 6666,
  sr_ioerr_vnode             = 6972,
  sr_ioerr_auth              = 7178,
  sr_ioerr_begin_atomic      = 7434,
  sr_ioerr_commit_atomic     = 7690,
  sr_ioerr_rollback_atomic   = 7946,
  sr_ioerr_data              = 8202

type SQLiteError* = object of CatchableError
  code: Option[ResultCode]

type SQLiteBlob* = object
  raw: ptr UncheckedArray[byte]
  len: int

proc raw*(blob: SQLiteBlob): ptr UncheckedArray[byte] = blob.raw
proc len*(blob: SQLiteBlob): int = blob.len

template toOpenArray*(blob: SQLiteBlob): untyped =
  blob.raw.toOpenArray(0, raw.len)

type OpenFlag* {.pure, size: sizeof(cint).} = enum
  so_readonly,
  so_readwrite,
  so_create,
  so_delete_on_close,
  so_exclusive,
  so_auto_proxy,
  so_uri,
  so_memory,
  so_main_db,
  so_temp_db,
  so_transient_db,
  so_main_journal,
  so_temp_journal,
  so_subjournal,
  so_super_journal,
  so_no_mutex,
  so_full_mutex,
  so_shared_cache,
  so_private_cache,
  so_wal,
  so_no_follow = 25

type OpenFlags* = set[OpenFlag]

type PrepareFlag* = enum
  sp_persistent,
  sp_normalize,
  sp_no_vtab

type PrepareFlags* = set[PrepareFlag]

type DatabaseEncoding* = enum
  enc_utf8,
  enc_utf16,
  enc_utf16be,
  enc_utf16le,

type SqliteDestroctor* = proc (p: pointer) {.cdecl.}

const StaticDestructor* = cast[SqliteDestroctor](0)
const TransientDestructor* = cast[SqliteDestroctor](-1)

type SqliteDateType* = enum
  dt_integer = 1,
  dt_float   = 2,
  dt_text    = 3,
  dt_blob    = 4,
  dt_null    = 5

type
  SqliteFileCtlOp* {.pure, size: sizeof(cint).} = enum
    sf_lockstate             = 1,
    sf_get_lockproxyfile     = 2,
    sf_set_lockproxyfile     = 3,
    sf_last_errno            = 4,
    sf_size_hint             = 5,
    sf_chunk_size            = 6,
    sf_file_pointer          = 7,
    sf_sync_omitted          = 8,
    sf_win32_av_retry        = 9,
    sf_persist_wal           = 10,
    sf_overwrite             = 11,
    sf_vfsname               = 12,
    sf_powersafe_overwrite   = 13,
    sf_pragma                = 14,
    sf_busyhandler           = 15,
    sf_tempfilename          = 16,
    sf_mmap_size             = 18,
    sf_trace                 = 19,
    sf_has_moved             = 20,
    sf_sync                  = 21,
    sf_commit_phasetwo       = 22,
    sf_win32_set_handle      = 23,
    sf_wal_block             = 24,
    sf_zipvfs                = 25,
    sf_rbu                   = 26,
    sf_vfs_pointer           = 27,
    sf_journal_pointer       = 28,
    sf_win32_get_handle      = 29,
    sf_pdb                   = 30,
    sf_begin_atomic_write    = 31,
    sf_commit_atomic_write   = 32,
    sf_rollback_atomic_write = 33,
    sf_lock_timeout          = 34,
    sf_data_version          = 35,
    sf_size_limit            = 36,
    sf_ckpt_done             = 37,
    sf_reserve_bytes         = 38,
    sf_ckpt_start            = 39,
    sf_external_reader       = 40,
    sf_cksm_file             = 41,
  SqliteLockLevel* {.pure, size: sizeof(cint).} = enum
    sl_none,
    sl_shared,
    sl_reserved,
    sl_pending,
    sl_exclusive
  SqliteAccessFlag* {.pure, size: sizeof(cint).} = enum
    access_exists,
    access_readwrite,
  SqliteShmLockFlag* {.pure, size: sizeof(cint).} = enum
    shm_unlock,
    shm_lock,
    shm_shared,
    shm_exclusive
  SqliteShmLockFlags* = set[SqliteShmLockFlag]
  SqliteDeviceCharacteristic* {.pure, size: sizeof(cint).} = enum
    dev_atomic,
    dev_atomic512,
    dev_atomic1k,
    dev_atomic2k,
    dev_atomic4k,
    dev_atomic8k,
    dev_atomic16k,
    dev_atomic32k,
    dev_atomic64k,
    dev_safe_append,
    dev_sequential,
    dev_undeletable_when_open,
    dev_powersafe_overwrite,
    dev_immutable,
    dev_batch_atomic
  SqliteDeviceCharacteristics* = set[SqliteDeviceCharacteristic]
  SqliteVFS* = object
    version*: cint
    osfilesize*: cint
    maxpathname*: cint
    next: ptr SqliteVFS
    name*: cstring
    appdata*: pointer

    open*           : proc (vfs: ptr SqliteVFS, name: cstring, file: ptr SqliteFile, flags: OpenFlags, outflags: ptr OpenFlags): ResultCode {.cdecl.}
    delete*         : proc (vfs: ptr SqliteVFS, name: cstring, syncDir: bool): ResultCode {.cdecl.}
    access*         : proc (vfs: ptr SqliteVFS, name: cstring, flag: SqliteAccessFlag, res: var bool): ResultCode {.cdecl.}
    fullpathname*   : proc (vfs: ptr SqliteVFS, name: cstring, nOut: cint, zOut: cstring): ResultCode {.cdecl.}
    dlopen*         : proc (vfs: ptr SqliteVFS, name: cstring): pointer {.cdecl.}
    dlerror*        : proc (vfs: ptr SqliteVFS, nByte: cint, zErrMsg: cstring): cint {.cdecl.}
    dlsym*          : proc (vfs: ptr SqliteVFS, lib: pointer, name: cstring): pointer {.cdecl.}
    dlclose*        : proc (vfs: ptr SqliteVFS, lib: pointer) {.cdecl.}
    randomness*     : proc (vfs: ptr SqliteVFS, nByte: cint, zOut: cstring): ResultCode {.cdecl.}
    sleep*          : proc (vfs: ptr SqliteVFS, microsecnods: cint): ResultCode {.cdecl.}
    currenttime*    : proc (vfs: ptr SqliteVFS, value: ptr float64): ResultCode {.cdecl.}
    getlasterror*   : proc (vfs: ptr SqliteVFS, nByte: cint, zOut: cstring): ResultCode {.cdecl.}
    currenttime64*  : proc (vfs: ptr SqliteVFS, value: ptr int64): ResultCode {.cdecl.}
    setsystemcall*  : proc (vfs: ptr SqliteVFS, name: cstring, p: pointer): ResultCode {.cdecl.}
    getsystemcall*  : proc (vfs: ptr SqliteVFS, name: cstring): pointer {.cdecl.}
    nextsystemcall* : proc (vfs: ptr SqliteVFS, name: cstring): cstring {.cdecl.}
  SqliteFile* = object
    vtable*: ptr SqliteIoMethods
  SqliteIoMethods* = object
    version*: cint

    close*      : proc (file: ptr SqliteFile): ResultCode {.cdecl.}
    read*       : proc (file: ptr SqliteFile, buffer: pointer, amt: cint, offset: int64): ResultCode {.cdecl.}
    write*      : proc (file: ptr SqliteFile, buffer: pointer, amt: cint, offset: int64): ResultCode {.cdecl.}
    truncate*   : proc (file: ptr SqliteFile, size: cint): ResultCode {.cdecl.}
    sync*       : proc (file: ptr SqliteFile, flags: cint): ResultCode {.cdecl.}
    size*       : proc (file: ptr SqliteFile, size: var int64): ResultCode {.cdecl.}
    lock*       : proc (file: ptr SqliteFile, level: SqliteLockLevel): ResultCode {.cdecl.}
    unlock*     : proc (file: ptr SqliteFile, level: SqliteLockLevel): ResultCode {.cdecl.}
    checklock*  : proc (file: ptr SqliteFile, outres: var bool): ResultCode {.cdecl.}
    filectl*    : proc (file: ptr SqliteFile, op: SqliteFileCtlOp, arg: pointer): ResultCode {.cdecl.}
    sectorsize* : proc (file: ptr SqliteFile): cint {.cdecl.}
    device*     : proc (file: ptr SqliteFile): SqliteDeviceCharacteristics {.cdecl.}
    shmmap*     : proc (file: ptr SqliteFile, pages: cint, pagesize: cint, extend: bool, target: var pointer): ResultCode {.cdecl.}
    shmlock*    : proc (file: ptr SqliteFile, offset: cint, n: cint, flags: SqliteShmLockFlags): ResultCode {.cdecl.}
    shmbarrier* : proc (file: ptr SqliteFile) {.cdecl.}
    shmunmap*   : proc (file: ptr SqliteFile, delete: bool): ResultCode {.cdecl.}
    fetch*      : proc (file: ptr SqliteFile, offset: int64, amt: cint, pp: var pointer): ResultCode {.cdecl.}
    unfetch*    : proc (file: ptr SqliteFile, offset: int64, p: pointer): ResultCode {.cdecl.}

proc sqlite3_auto_extension*(entry: pointer): ResultCode {.sqlite3linkage.}
proc sqlite3_vfs_find*(name: cstring): ptr SqliteVFS {.sqlite3linkage.}
proc sqlite3_vfs_register*(vfs: ptr SqliteVFS, default: bool): ResultCode {.sqlite3linkage.}
proc sqlite3_malloc*(size: cint): pointer {.sqlite3linkage.}
proc sqlite3_malloc64*(size: uint64): pointer {.sqlite3linkage.}
proc sqlite3_realloc*(src: pointer, size: cint): pointer {.sqlite3linkage.}
proc sqlite3_realloc64*(src: pointer, size: uint64): pointer {.sqlite3linkage.}
proc sqlite3_free*(src: pointer) {.sqlite3linkage.}
proc sqlite3_msize*(src: pointer): uint64 {.sqlite3linkage.}
proc sqlite3_mprintf*(fmt: cstring): cstring {.sqlite3linkage, varargs.}
proc sqlite3_snprintf*(size: cint, target: cstring, fmt: cstring): cstring {.sqlite3linkage, varargs.}
proc sqlite3_errmsg*(db: ptr RawDatabase): cstring {.sqlite3linkage.}
proc sqlite3_errstr*(code: ResultCode): cstring {.sqlite3linkage.}
proc sqlite3_db_handle*(st: ptr RawStatement): ptr RawDatabase {.sqlite3linkage.}
proc sqlite3_enable_shared_cache*(enabled: int): ResultCode {.sqlite3linkage.}
proc sqlite3_open_v2*(filename: cstring, db: ptr ptr RawDatabase, flags: OpenFlags, vfs: cstring): ResultCode {.sqlite3linkage.}
proc sqlite3_close_v2*(db: ptr RawDatabase): ResultCode {.sqlite3linkage.}
proc sqlite3_prepare_v3*(db: ptr RawDatabase, sql: cstring, nbyte: int, flags: PrepareFlags, pstmt: ptr ptr RawStatement, tail: ptr cstring): ResultCode {.sqlite3linkage.}
proc sqlite3_finalize*(st: ptr RawStatement): ResultCode {.sqlite3linkage.}
proc sqlite3_reset*(st: ptr RawStatement): ResultCode {.sqlite3linkage.}
proc sqlite3_step*(st: ptr RawStatement): ResultCode {.sqlite3linkage.}
proc sqlite3_bind_parameter_index*(st: ptr RawStatement, name: cstring): int {.sqlite3linkage.}
proc sqlite3_bind_blob64*(st: ptr RawStatement, idx: int, buffer: pointer, len: int, free: SqliteDestroctor): ResultCode {.sqlite3linkage.}
proc sqlite3_bind_double*(st: ptr RawStatement, idx: int, value: float64): ResultCode {.sqlite3linkage.}
proc sqlite3_bind_int64*(st: ptr RawStatement, idx: int, val: int64): ResultCode {.sqlite3linkage.}
proc sqlite3_bind_null*(st: ptr RawStatement, idx: int): ResultCode {.sqlite3linkage.}
proc sqlite3_bind_text*(st: ptr RawStatement, idx: int, val: cstring, len: int32, free: SqliteDestroctor): ResultCode {.sqlite3linkage.}
proc sqlite3_bind_value*(st: ptr RawStatement, idx: int, val: ptr RawValue): ResultCode {.sqlite3linkage.}
proc sqlite3_bind_pointer*(st: ptr RawStatement, idx: int, val: pointer, name: cstring, free: SqliteDestroctor): ResultCode {.sqlite3linkage.}
proc sqlite3_bind_zeroblob64*(st: ptr RawStatement, idx: int, len: int): ResultCode {.sqlite3linkage.}
proc sqlite3_changes*(st: ptr RawDatabase): int {.sqlite3linkage.}
proc sqlite3_last_insert_rowid*(st: ptr RawDatabase): int {.sqlite3linkage.}
proc sqlite3_column_type*(st: ptr RawStatement, idx: int): SqliteDateType {.sqlite3linkage.}
proc sqlite3_column_blob*(st: ptr RawStatement, idx: int): pointer {.sqlite3linkage.}
proc sqlite3_column_bytes*(st: ptr RawStatement, idx: int): int {.sqlite3linkage.}
proc sqlite3_column_double*(st: ptr RawStatement, idx: int): float64 {.sqlite3linkage.}
proc sqlite3_column_int64*(st: ptr RawStatement, idx: int): int64 {.sqlite3linkage.}
proc sqlite3_column_text*(st: ptr RawStatement, idx: int): cstring {.sqlite3linkage.}
proc sqlite3_column_value*(st: ptr RawStatement, idx: int): ptr RawValue {.sqlite3linkage.}

proc newSQLiteError*(code: ResultCode): ref SQLiteError =
  result = newException(SQLiteError, $sqlite3_errstr code)
  result.code = some code

proc newSQLiteError*(db: ptr RawDatabase): ref SQLiteError =
  newException(SQLiteError, $sqlite3_errmsg db)

proc newSQLiteError*(db: ptr RawStatement): ref SQLiteError =
  newException(SQLiteError, $sqlite3_errmsg sqlite3_db_handle db)

template sqliteCheck*(res: ResultCode) =
  let tmp = res
  if tmp != ResultCode.sr_ok:
    raise newSQLiteError tmp

template sqliteCheck*(db: ptr RawDatabase, res: ResultCode) =
  let tmp = res
  if tmp != ResultCode.sr_ok:
    raise newSQLiteError db

template sqliteCheck*(st: ptr RawStatement, res: ResultCode) =
  let tmp = res
  if tmp != ResultCode.sr_ok:
    raise newSQLiteError st

proc `=destroy`*(st: var Statement) =
  if st.raw != nil:
    sqliteCheck sqlite3_finalize st.raw

preventCopy Statement

proc `=destroy`*(db: var Database) =
  if db.raw != nil:
    db.stmtcache.clear()
    sqliteCheck sqlite3_close_v2 db.raw

preventCopy Database

proc enableSharedCache*(enabled: bool = true) =
  sqliteCheck sqlite3_enable_shared_cache(if enabled: 1 else: 0)

proc initDatabase*(
  filename: string,
  flags: OpenFlags = {so_readwrite, so_create},
  vfs: cstring = nil
): Database =
  sqliteCheck sqlite3_open_v2(filename, addr result.raw, flags, vfs)
  result.stmtcache = initTable[CachedHash[string], ref Statement]()

proc changes*(st: var Database): int =
  sqlite3_changes st.raw

proc changes*(st: ref Statement): int =
  sqlite3_changes sqlite3_db_handle st.raw

proc newStatement*(db: var Database, sql: string, flags: PrepareFlags = {}): ref Statement =
  new result
  sqliteCheck db.raw, sqlite3_prepare_v3(db.raw, sql, sql.len, flags,
      addr result.raw, nil)

proc fetchStatement*(db: var Database, sql: string): ref Statement =
  let rhash = cacheHash(sql)
  db.stmtcache.withValue(rhash, value) do:
    return value[]
  do:
    result = db.newStatement(sql, {sp_persistent})
    db.stmtcache[rhash] = result

proc fetchStatement*(db: var Database, sql: static[string]): ref Statement =
  const chash = sql.compileTimeHash
  db.stmtcache.withValue(chash, value) do:
    return value[]
  do:
    result = db.newStatement(sql, {sp_persistent})
    db.stmtcache[chash] = result

proc getParameterIndex*(st: ref Statement, name: string): int =
  result = sqlite3_bind_parameter_index(st.raw, name)
  if result == 0:
    raise newException(SQLiteError, "Unknown parameter " & name)

{.push inline.}

proc `[]=`*(st: ref Statement, idx: int, blob: openarray[byte]) =
  st.raw.sqliteCheck sqlite3_bind_blob64(st.raw, idx, blob.unsafeAddr, blob.len, TransientDestructor)

proc `[]=`*(st: ref Statement, idx: int, val: SomeFloat) =
  st.raw.sqliteCheck sqlite3_bind_double(st.raw, idx, float64 val)

proc `[]=`*(st: ref Statement, idx: int, val: SomeOrdinal) =
  st.raw.sqliteCheck sqlite3_bind_int64(st.raw, idx, cast[int64](val))

proc `[]=`*(st: ref Statement, idx: int, val: type(nil)) =
  st.raw.sqliteCheck sqlite3_bind_null(st.raw, idx)

proc `[]=`*(st: ref Statement, idx: int, val: string) =
  st.raw.sqliteCheck sqlite3_bind_text(st.raw, idx, val, int32 val.len, TransientDestructor)

proc reset*(st: ref Statement) =
  st.raw.sqliteCheck sqlite3_reset(st.raw)

proc step*(st: ref Statement): bool {.inline.} =
  let res = sqlite3_step(st.raw)
  case res:
  of sr_row: true
  of sr_done: false
  else: raise newSQLiteError(st.raw)

proc lastInsertRowid*(st: var Database): int =
  sqlite3_last_insert_rowid(st.raw)

proc withColumnBlob*(st: ref Statement, idx: int, recv: proc(vm: openarray[byte])) =
  let p = sqlite3_column_blob(st.raw, idx)
  let l = sqlite3_column_bytes(st.raw, idx)
  recv(cast[ptr UncheckedArray[byte]](p).toOpenArray(0, l))

proc getColumnType*(st: ref Statement, idx: int): SqliteDateType =
  sqlite3_column_type(st.raw, idx)

proc getColumn*(st: ref Statement, idx: int, T: typedesc[seq[byte]]): seq[byte] =
  let p = cast[ptr UncheckedArray[byte]](sqlite3_column_blob(st.raw, idx))
  let l = sqlite3_column_bytes(st.raw, idx)
  result = newSeq[byte]l
  copyMem(addr result[0], p, l)

proc getColumn*(st: ref Statement, idx: int, T: typedesc[SomeFloat]): SomeFloat =
  cast[T](sqlite3_column_double(st.raw, idx))

proc getColumn*(st: ref Statement, idx: int, T: typedesc[SomeOrdinal]): SomeOrdinal =
  cast[T](sqlite3_column_int64(st.raw, idx))

proc getColumn*(st: ref Statement, idx: int, T: typedesc[string]): string =
  let p = sqlite3_column_text(st.raw, idx)
  let l = sqlite3_column_bytes(st.raw, idx)
  result = newString l
  copyMem(addr result[0], p, l)

proc getColumn*[T](st: ref Statement, idx: int, _: typedesc[Option[T]]): Option[T] =
  if st.getColumnType(idx) == dt_null:
    none(T)
  else:
    st.getColumn(idx, T)

proc unpack*[T: tuple](st: ref Statement, _: typedesc[T]): T =
  var idx = 0
  for value in result.fields:
    value = st.getColumn(idx, type(value))
    idx.inc

{.pop.}

proc exec*(db: var Database, sql: string, cache: static bool = true): int {.discardable.} =
  when cache:
    let st = db.fetchStatement(sql)
    defer: st.reset()
  else:
    let st = db.newStatement(sql)
  if st.step():
    result = st.getColumn(0, int)

proc execM*(db: var Database, sqls: varargs[string]) {.discardable.} =
  discard db.exec "BEGIN IMMEDIATE"
  try:
    for sql in sqls:
      discard db.exec(sql, cache = false)
    discard db.exec "COMMIT"
  except:
    discard db.exec "ROLLBACK"
    raise getCurrentException()