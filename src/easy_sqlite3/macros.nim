import std/[macros, options]
import ./bindings, ./utils

proc injectDbDecl(result: var NimNode, db_ident: NimNode) =
  result[3].insert(1, nnkIdentDefs.newTree(
    db_ident,
    nnkVarTy.newTree(bindSym "Database"),
    newEmptyNode()
  ))

proc injectDbFetch(procbody: var NimNode, sql: string, db_ident, st_ident: NimNode) =
  procbody.add nnkVarSection.newTree(
    nnkIdentDefs.newTree(
      st_ident,
      newEmptyNode(),
      nnkCall.newTree(
        nnkDotExpr.newTree(
          db_ident,
          bindSym "fetchStatement"
        ),
        newLit sql
      )
    )
  )

proc injectDbArguments(procbody: var NimNode, body, st_ident: NimNode): seq[tuple[name: string, idxnode, param: NimNode]] =
  result = newSeq[tuple[name: string, idxnode, param: NimNode]]()
  procbody.addTree(nnkVarSection, varsec):
    for args in body[3][1..^1]:
      for arg in args[0..^3]:
        let arg_name = $arg
        let arg_ident = genSym(nskVar, arg_name & "_idx")
        result.add (name: arg_name, idxnode: arg_ident, param: arg)
        varsec.add nnkIdentDefs.newTree(
          nnkPragmaExpr.newTree(
            arg_ident,
            nnkPragma.newTree(
              ident "threadvar"
            )
          ),
          ident "int",
          newEmptyNode()
        )
  for it in result:
    procbody.add nnkIfStmt.newTree(
      nnkElifBranch.newTree(
        nnkInfix.newTree(
          ident "==",
          it.idxnode,
          newLit 0
        ),
        nnkStmtList.newTree(
          nnkAsgn.newTree(
            it.idxnode,
            nnkCall.newTree(
              nnkDotExpr.newTree(
                st_ident,
                bindSym "getParameterIndex"
              ),
              newLit "$" & it.name
            )
          )
        )
      )
    )
  # procbody.add nnkCall.newTree(
  #   nnkDotExpr.newTree(
  #     st_ident,
  #     bindSym "reset"
  #   )
  # )
  for it in result:
    procbody.add nnkAsgn.newTree(
      nnkBracketExpr.newTree(
        st_ident,
        it.idxnode
      ),
      it.param
    )

proc fillPar(ret, st_ident: NimNode): NimNode =
  nnkPar.genTree(parbody):
    for idx, it in ret:
      parbody.add nnkExprColonExpr.newTree(
        it[0],
        nnkCall.newTree(
          nnkDotExpr.newTree(
            st_ident,
            bindSym "getColumn"
          ),
          newLit idx,
          nnkBracketExpr.newTree(newIdentNode("typedesc"),it[1])
        )
      )

proc genQueryIterator(sql: string, body: NimNode): NimNode =
  result = body.copy()
  let db_ident = genSym(nskParam, "db")
  let st_ident = genSym(nskVar, "st")

  let rettype = block:
    case result[3][0].kind
    of nnkTupleTy:
      result[3][0]
    of nnkSym:
      let typeDef = result[3][0].getImpl
      typeDef.expectKind nnkTypeDef
      let tuplDef = typeDef[2]
      tuplDef.expectKind nnkTupleTy
      tuplDef
    else:
      error "Expected a tuple type", result[3][0]
      nil
  
  injectDbDecl(result, db_ident)
  result[6] = nnkStmtList.genTree(procbody):
    injectDbFetch(procbody, sql, db_ident, st_ident)
    discard injectDbArguments(procbody, body, st_ident)
    procbody.addTree(nnkTryStmt, trybody):
      trybody.addTree(nnkWhileStmt, whilebody):
        whilebody.add nnkCall.newTree(nnkDotExpr.newTree(st_ident, bindSym "step"))
        whilebody.addTree(nnkYieldStmt, yieldbody):
          yieldbody.add fillPar(rettype, st_ident)
      trybody.addTree(nnkFinally, finallybody):
        finallybody.add nnkCall.newTree(
          nnkDotExpr.newTree(
            st_ident,
            bindSym "reset"
          )
        )

proc genQueryProcedure(sql: string, body, tupdef: NimNode, opt: static bool): NimNode =
  result = body.copy()
  let db_ident = genSym(nskParam, "db")
  let st_ident = genSym(nskVar, "st")
  let rettype = when opt:
    result[3][0][1]
  else:
    tupdef
  injectDbDecl(result, db_ident)
  result[6] = nnkStmtList.genTree(procbody):
    injectDbFetch(procbody, sql, db_ident, st_ident)
    discard injectDbArguments(procbody, body, st_ident)
    procbody.addTree(nnkTryStmt, trybody):
      trybody.addTree(nnkIfStmt, ifbody):
        ifbody.addTree(nnkElifBranch, branch):
          branch.add nnkCall.newTree(nnkDotExpr.newTree(st_ident, bindSym "step"))
          branch.addTree(nnkStmtList, resultstmt):
            resultstmt.addTree(nnkAsgn, retbody):
              retbody.add ident "result"
              let tmp = fillPar(rettype, st_ident)
              when opt:
                retbody.add nnkCommand.newTree(bindSym "some", tmp)
              else:
                retbody.add tmp
            resultstmt.addTree(nnkIfStmt, ifbody2):
              ifbody2.addTree(nnkElifBranch, dup_branch):
                dup_branch.add nnkCall.newTree(nnkDotExpr.newTree(st_ident, bindSym "step"))
                dup_branch.add nnkRaiseStmt.newTree(
                  nnkCall.newTree(bindSym "newException", ident "SQLiteError", newLit "Too many results")
                )
        ifbody.addTree(nnkElse, elsebody):
          when opt:
            elsebody.add nnkReturnStmt.newTree(
              nnkCommand.newTree(bindSym "none", rettype)
            )
          else:
            elsebody.add nnkRaiseStmt.newTree(
              nnkCall.newTree(bindSym "newException", ident "SQLiteError", newLit "No results")
            )
      trybody.addTree(nnkFinally, finallybody):
        finallybody.add nnkCall.newTree(
          nnkDotExpr.newTree(
            st_ident,
            bindSym "reset"
          )
        )

proc genUpdateProcedure(sql: string, body: NimNode): NimNode =
  result = body.copy()
  let db_ident = genSym(nskParam, "db")
  let st_ident = genSym(nskVar, "st")
  injectDbDecl(result, db_ident)
  result[6] = nnkStmtList.genTree(procbody):
    injectDbFetch(procbody, sql, db_ident, st_ident)
    discard injectDbArguments(procbody, body, st_ident)
    procbody.addTree(nnkTryStmt, trybody):
      trybody.addTree(nnkIfStmt, ifbody):
        ifbody.addTree(nnkElifBranch, branch):
          branch.add nnkCall.newTree(nnkDotExpr.newTree(st_ident, bindSym "step"))
          branch.add nnkRaiseStmt.newTree(
            nnkCall.newTree(bindSym "newException", ident "SQLiteError", newLit "Invalid update")
          )
        ifbody.addTree(nnkElse, elsebody):
          elsebody.add nnkReturnStmt.newTree(
            nnkCall.newTree(nnkDotExpr.newTree(db_ident, bindSym "lastInsertRowid"))
          )
      trybody.addTree(nnkFinally, finallybody):
        finallybody.add nnkCall.newTree(
          nnkDotExpr.newTree(
            st_ident,
            bindSym "reset"
          )
        )

proc genCreateProcedure(sql: string, body: NimNode): NimNode =
  result = body.copy()
  let db_ident = genSym(nskParam, "db")
  let st_ident = genSym(nskVar, "st")
  injectDbDecl(result, db_ident)
  result[6] = nnkStmtList.genTree(procbody):
    injectDbFetch(procbody, sql, db_ident, st_ident)
    discard injectDbArguments(procbody, body, st_ident)
    procbody.addTree(nnkTryStmt, trybody):
      trybody.addTree(nnkIfStmt, ifbody):
        ifbody.addTree(nnkElifBranch, branch):
          branch.add nnkCall.newTree(nnkDotExpr.newTree(st_ident, bindSym "step"))
          branch.add nnkRaiseStmt.newTree(
            nnkCall.newTree(bindSym "newException", ident "SQLiteError", newLit "Invalid statement")
          )
      trybody.addTree(nnkFinally, finallybody):
        finallybody.add nnkCall.newTree(
          nnkDotExpr.newTree(
            st_ident,
            bindSym "reset"
          )
        )

macro importdb*(sql: static string, body: typed) =
  case body.kind:
  of nnkProcDef:
    let ret = body[3][0]
    case ret.kind:
    of nnkEmpty:
      result = genCreateProcedure(sql, body)
    of nnkSym:
      case ret.strVal:
      of "int":
        result = genUpdateProcedure(sql, body)
      else:
        let typImpl = ret.getImpl
        typImpl.expectKind nnkTypeDef
        let tuplImpl = typImpl[2]
        tuplImpl.expectKind nnkTupleTy
        result = genQueryProcedure(sql, body, tuplImpl, false)
    of nnkBracketExpr:
      ret[0].expectIdent "Option"
      ret[1].expectKind nnkTupleTy
      result = genQueryProcedure(sql, body, ret[1], true)
    of nnkTupleTy:
      result = genQueryProcedure(sql, body, ret, false)
    else:
      error("Expected int, tuple, Option[tuple]")
      return
  of nnkIteratorDef:
    let retType = body[3][0]
    case retType.kind
    of nnkTupleTy, nnkSym:
      result = genQueryIterator(sql, body)
    else:
      error("Expected a tuple type", retType)
  else:
    error("Expected proc or iterator, got " & $body.kind, body)
    return
  
  if result[0].isExported:
    result[0] = nnkPostfix.newTree(
      "*".ident,
      result[0].strVal.ident
    )
  else:
    result[0] = result[0].strVal.ident

proc db_begin_deferred() {.importdb: "BEGIN DEFERRED".}
proc db_begin_immediate() {.importdb: "BEGIN IMMEDIATE".}
proc db_begin_exclusive() {.importdb: "BEGIN EXCLUSIVE".}
proc db_commit() {.importdb: "COMMIT".}
proc db_rollback() {.importdb: "ROLLBACK".}

template gen_transaction(db: var Database, begin_stmt, body: untyped): untyped =
  begin_stmt db
  block outer:
    template commit() {.inject, used.} =
      db_commit db
      break outer
    template rollback() {.inject, used.} =
      db_rollback db
      break outer
    block inner:
      try:
        body
        commit()
      except CatchableError:
        db_rollback db
        raise getCurrentException()

template transaction*(db: var Database, body: untyped): untyped =
  gen_transaction db, db_begin_deferred, body

template transactionImmediate*(db: var Database, body: untyped): untyped =
  gen_transaction db, db_begin_immediate, body

template transactionExclusive*(db: var Database, body: untyped): untyped =
  gen_transaction db, db_begin_exclusive, body
