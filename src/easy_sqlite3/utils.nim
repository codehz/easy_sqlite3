import std/macros

template disallow_copy*(T: untyped): untyped =
  proc `=copy`*(l: var T, r: T) {.error.}

template genTree*(kind: NimNodeKind, local, body: untyped): NimNode =
  var local {.gensym.} = kind.newNimNode()
  body
  local

template addTree*(src: NimNode, kind: NimNodeKind, local, body: untyped) =
  var local {.gensym.} = kind.newNimNode()
  body
  src.add local

proc getNimIdent*(src: NimNode): string =
  case src.kind:
  of nnkIdent:
    return src.strVal
  of nnkPostfix:
    src[0].expectIdent "*"
    src[1].expectKind nnkIdent
    return src[1].strVal
  else:
    error("Not an ident node")