///|
/// Abstract Syntax Tree (AST) definitions for streaming SQL queries.
///
/// Models expressions, window specifications, projections, and complete continuous queries.

///|
/// Expression node in a streaming SQL query.
pub enum SqlExpr {
  ColRef(String)
  LiteralDouble(Double)
  LiteralInt(Int64)
  LiteralString(String)
  BinaryOp(String, SqlExpr, SqlExpr)
  FuncCall(String, Array[SqlExpr])
}

///|
pub impl Eq for SqlExpr with equal(self, other) {
  match (self, other) {
    (ColRef(a), ColRef(b)) => a == b
    (LiteralDouble(a), LiteralDouble(b)) => a == b
    (LiteralInt(a), LiteralInt(b)) => a == b
    (LiteralString(a), LiteralString(b)) => a == b
    (BinaryOp(op1, l1, r1), BinaryOp(op2, l2, r2)) =>
      op1 == op2 && l1 == l2 && r1 == r2
    (FuncCall(f1, args1), FuncCall(f2, args2)) => {
      if f1 != f2 || args1.length() != args2.length() {
        false
      } else {
        let mut same = true
        for i = 0; i < args1.length(); i = i + 1 {
          if args1[i] != args2[i] {
            same = false
            break
          }
        }
        same
      }
    }
    _ => false
  }
}

///|
pub impl Eq for SqlExpr with not_equal(self, other) {
  !(self == other)
}

///|
pub extend SqlExpr with Eq::{equal, not_equal}

///|
/// Streaming window specification attached to a continuous query.
pub enum WindowSpec {
  Tumble(String, Int64)
  Hop(String, Int64, Int64)
  Session(String, Int64)
}

///|
pub impl Eq for WindowSpec with equal(self, other) {
  match (self, other) {
    (Tumble(c1, s1), Tumble(c2, s2)) => c1 == c2 && s1 == s2
    (Hop(c1, s1, l1), Hop(c2, s2, l2)) => c1 == c2 && s1 == s2 && l1 == l2
    (Session(c1, g1), Session(c2, g2)) => c1 == c2 && g1 == g2
    _ => false
  }
}

///|
pub impl Eq for WindowSpec with not_equal(self, other) {
  !(self == other)
}

///|
pub extend WindowSpec with Eq::{equal, not_equal}

///|
/// Projected field in a SELECT clause with optional column alias.
pub struct SelectField {
  expr : SqlExpr
  col_alias : String?
}

///|
pub impl Eq for SelectField with equal(self, other) {
  self.expr == other.expr && self.col_alias == other.col_alias
}

///|
pub impl Eq for SelectField with not_equal(self, other) {
  !(self == other)
}

///|
pub extend SelectField with Eq::{equal, not_equal}

///|
/// Creates a SelectField.
pub fn SelectField::new(expr : SqlExpr, col_alias : String?) -> SelectField {
  { expr, col_alias }
}

///|
/// Top-level AST representation of a continuous streaming SQL query.
pub struct StreamingQuery {
  select_fields : Array[SelectField]
  source_stream : String
  window : WindowSpec?
  where_clause : SqlExpr?
  group_by_keys : Array[String]
}

///|
/// Creates a StreamingQuery.
pub fn StreamingQuery::new(
  select_fields : Array[SelectField],
  source_stream : String,
  window : WindowSpec?,
  where_clause : SqlExpr?,
  group_by_keys : Array[String],
) -> StreamingQuery {
  { select_fields, source_stream, window, where_clause, group_by_keys }
}

///|
/// Accesses projected fields.
pub fn StreamingQuery::get_select_fields(
  self : StreamingQuery,
) -> Array[SelectField] {
  self.select_fields
}

///|
/// Accesses source stream name.
pub fn StreamingQuery::get_source_stream(self : StreamingQuery) -> String {
  self.source_stream
}

///|
/// Accesses optional window specification.
pub fn StreamingQuery::get_window(self : StreamingQuery) -> WindowSpec? {
  self.window
}

///|
/// Accesses optional filter predicate expression.
pub fn StreamingQuery::get_where_clause(self : StreamingQuery) -> SqlExpr? {
  self.where_clause
}

///|
/// Accesses group-by partition keys.
pub fn StreamingQuery::get_group_by_keys(
  self : StreamingQuery,
) -> Array[String] {
  self.group_by_keys
}
