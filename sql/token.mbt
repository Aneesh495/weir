///|
/// Lexical tokens for continuous streaming SQL (WeirSQL).
///
/// Encapsulates keywords, identifiers, literal values, and punctuation symbols
/// recognized by the streaming query parser.

///|
/// Enumeration of all streaming SQL token variants.
pub enum TokenKind {
  // Keywords
  KwSelect
  KwFrom
  KwWhere
  KwGroup
  KwBy
  KwWindow
  KwTumble
  KwHop
  KwSession
  KwInterval
  KwSecond
  KwMinute
  KwHour
  KwEmit
  KwChanges
  KwWatermark
  KwAnd
  KwOr
  KwNot
  KwAs

  // Symbols
  Comma
  Dot
  Star
  LParen
  RParen
  OpEq
  OpNeq
  OpGt
  OpLt
  OpGte
  OpLte
  OpPlus
  OpMinus

  // Literals & Identifiers
  Ident(String)
  StrLit(String)
  IntLit(Int64)
  DblLit(Double)
  Eof
}

///|
pub impl Eq for TokenKind with equal(self, other) {
  match (self, other) {
    (KwSelect, KwSelect) => true
    (KwFrom, KwFrom) => true
    (KwWhere, KwWhere) => true
    (KwGroup, KwGroup) => true
    (KwBy, KwBy) => true
    (KwWindow, KwWindow) => true
    (KwTumble, KwTumble) => true
    (KwHop, KwHop) => true
    (KwSession, KwSession) => true
    (KwInterval, KwInterval) => true
    (KwSecond, KwSecond) => true
    (KwMinute, KwMinute) => true
    (KwHour, KwHour) => true
    (KwEmit, KwEmit) => true
    (KwChanges, KwChanges) => true
    (KwWatermark, KwWatermark) => true
    (KwAnd, KwAnd) => true
    (KwOr, KwOr) => true
    (KwNot, KwNot) => true
    (KwAs, KwAs) => true
    (Comma, Comma) => true
    (Dot, Dot) => true
    (Star, Star) => true
    (LParen, LParen) => true
    (RParen, RParen) => true
    (OpEq, OpEq) => true
    (OpNeq, OpNeq) => true
    (OpGt, OpGt) => true
    (OpLt, OpLt) => true
    (OpGte, OpGte) => true
    (OpLte, OpLte) => true
    (OpPlus, OpPlus) => true
    (OpMinus, OpMinus) => true
    (Ident(a), Ident(b)) => a == b
    (StrLit(a), StrLit(b)) => a == b
    (IntLit(a), IntLit(b)) => a == b
    (DblLit(a), DblLit(b)) => a == b
    (Eof, Eof) => true
    _ => false
  }
}

///|
pub impl Eq for TokenKind with not_equal(self, other) {
  !(self == other)
}

///|
pub extend TokenKind with Eq::{equal, not_equal}

///|
pub impl Show for TokenKind with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for TokenKind with to_string(self) {
  match self {
    KwSelect => "SELECT"
    KwFrom => "FROM"
    KwWhere => "WHERE"
    KwGroup => "GROUP"
    KwBy => "BY"
    KwWindow => "WINDOW"
    KwTumble => "TUMBLE"
    KwHop => "HOP"
    KwSession => "SESSION"
    KwInterval => "INTERVAL"
    KwSecond => "SECOND"
    KwMinute => "MINUTE"
    KwHour => "HOUR"
    KwEmit => "EMIT"
    KwChanges => "CHANGES"
    KwWatermark => "WATERMARK"
    KwAnd => "AND"
    KwOr => "OR"
    KwNot => "NOT"
    KwAs => "AS"
    Comma => ","
    Dot => "."
    Star => "*"
    LParen => "("
    RParen => ")"
    OpEq => "="
    OpNeq => "!="
    OpGt => ">"
    OpLt => "<"
    OpGte => ">="
    OpLte => "<="
    OpPlus => "+"
    OpMinus => "-"
    Ident(id) => "Ident(" + id + ")"
    StrLit(s) => "StrLit(" + s + ")"
    IntLit(i) => "IntLit(" + i.to_string() + ")"
    DblLit(d) => "DblLit(" + d.to_string() + ")"
    Eof => "<EOF>"
  }
}

///|
pub extend TokenKind with Show::{output, to_string}

///|
/// Token container pairing token variant with character offset.
pub struct Token {
  kind : TokenKind
  pos : Int
}

///|
/// Creates a new Token.
pub fn Token::new(kind : TokenKind, pos : Int) -> Token {
  { kind, pos }
}
