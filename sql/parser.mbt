///|
/// Recursive descent parser for streaming continuous queries (WeirSQL).
///
/// Parses token sequences into typed StreamingQuery AST instances with syntactic validation.

///|
/// Parser state holding token array and current cursor index.
pub struct SqlParser {
  tokens : Array[Token]
  mut cursor : Int
}

///|
/// Creates a new SqlParser for a token stream.
pub fn SqlParser::new(tokens : Array[Token]) -> SqlParser {
  { tokens, cursor: 0 }
}

///|
/// Peeks at current token kind.
fn SqlParser::peek(self : SqlParser) -> TokenKind {
  if self.cursor < self.tokens.length() {
    self.tokens[self.cursor].kind
  } else {
    Eof
  }
}

///|
/// Advances and returns the current token.
fn SqlParser::advance(self : SqlParser) -> TokenKind {
  let k = self.peek()
  if self.cursor < self.tokens.length() {
    self.cursor = self.cursor + 1
  }
  k
}

///|
/// Checks if current token matches kind. If so, advances and returns true.
fn SqlParser::match_token(self : SqlParser, kind : TokenKind) -> Bool {
  if self.peek() == kind {
    let _ = self.advance()
    true
  } else {
    false
  }
}

///|
/// Parses primary expression: identifier, literal, or parenthesized expr.
fn SqlParser::parse_primary(self : SqlParser) -> SqlExpr? {
  match self.advance() {
    Ident(id) => {
      if self.peek() == LParen {
        let _ = self.advance()
        let args : Array[SqlExpr] = []
        if self.peek() != RParen {
          match self.parse_primary() {
            Some(arg) => args.push(arg)
            None => ()
          }
          while self.match_token(Comma) {
            match self.parse_primary() {
              Some(arg) => args.push(arg)
              None => ()
            }
          }
        }
        let _ = self.match_token(RParen)
        Some(FuncCall(id, args))
      } else {
        Some(ColRef(id))
      }
    }
    StrLit(s) => Some(LiteralString(s))
    IntLit(i) => Some(LiteralInt(i))
    DblLit(d) => Some(LiteralDouble(d))
    Star => Some(ColRef("*"))
    LParen => {
      let inner = self.parse_expression()
      let _ = self.match_token(RParen)
      inner
    }
    _ => None
  }
}

///|
/// Parses binary comparison or arithmetic expression.
fn SqlParser::parse_expression(self : SqlParser) -> SqlExpr? {
  match self.parse_primary() {
    None => None
    Some(left) => {
      let op_str = match self.peek() {
        OpEq => "="
        OpNeq => "!="
        OpGt => ">"
        OpLt => "<"
        OpGte => ">="
        OpLte => "<="
        OpPlus => "+"
        OpMinus => "-"
        KwAnd => "AND"
        KwOr => "OR"
        _ => ""
      }

      if op_str != "" {
        let _ = self.advance()
        match self.parse_expression() {
          Some(right) => Some(BinaryOp(op_str, left, right))
          None => Some(left)
        }
      } else {
        Some(left)
      }
    }
  }
}

///|
/// Parses a complete StreamingQuery from tokens.
pub fn SqlParser::parse(self : SqlParser) -> StreamingQuery? {
  if !self.match_token(KwSelect) {
    return None
  }

  // 1. Parse projected fields
  let fields : Array[SelectField] = []
  match self.parse_expression() {
    Some(e) => {
      let mut col_alias : String? = None
      if self.match_token(KwAs) {
        match self.advance() {
          Ident(a) => col_alias = Some(a)
          _ => ()
        }
      }
      fields.push(SelectField::new(e, col_alias))
    }
    None => return None
  }

  while self.match_token(Comma) {
    match self.parse_expression() {
      Some(e) => {
        let mut col_alias : String? = None
        if self.match_token(KwAs) {
          match self.advance() {
            Ident(a) => col_alias = Some(a)
            _ => ()
          }
        }
        fields.push(SelectField::new(e, col_alias))
      }
      None => break
    }
  }

  // 2. Parse FROM <stream>
  if !self.match_token(KwFrom) {
    return None
  }

  let source_stream = match self.advance() {
    Ident(name) => name
    _ => return None
  }

  // 3. Optional WINDOW clause
  let mut window_spec : WindowSpec? = None
  if self.match_token(KwWindow) {
    let win_kind = self.advance()
    if !self.match_token(LParen) {
      return None
    }
    let col = match self.advance() {
      Ident(c) => c
      _ => return None
    }
    if !self.match_token(Comma) || !self.match_token(KwInterval) {
      return None
    }
    let sz = match self.advance() {
      IntLit(n) => n
      _ => return None
    }
    let _ = self.advance() // SECOND / MINUTE / etc.

    match win_kind {
      KwTumble => {
        let _ = self.match_token(RParen)
        window_spec = Some(Tumble(col, sz))
      }
      KwHop => {
        let mut slide_sz = sz
        if self.match_token(Comma) && self.match_token(KwInterval) {
          match self.advance() {
            IntLit(s) => slide_sz = s
            _ => ()
          }
          let _ = self.advance()
        }
        let _ = self.match_token(RParen)
        window_spec = Some(Hop(col, sz, slide_sz))
      }
      KwSession => {
        let _ = self.match_token(RParen)
        window_spec = Some(Session(col, sz))
      }
      _ => ()
    }
  }

  // 4. Optional WHERE clause
  let mut where_clause : SqlExpr? = None
  if self.match_token(KwWhere) {
    where_clause = self.parse_expression()
  }

  // 5. Optional GROUP BY clause
  let group_keys : Array[String] = []
  if self.match_token(KwGroup) && self.match_token(KwBy) {
    match self.advance() {
      Ident(k) => group_keys.push(k)
      _ => ()
    }
    while self.match_token(Comma) {
      match self.advance() {
        Ident(k) => group_keys.push(k)
        _ => ()
      }
    }
  }

  // 6. Optional EMIT CHANGES clause
  if self.match_token(KwEmit) {
    let _ = self.match_token(KwChanges)
  }

  Some(StreamingQuery::new(
    fields,
    source_stream,
    window_spec,
    where_clause,
    group_keys,
  ))
}
