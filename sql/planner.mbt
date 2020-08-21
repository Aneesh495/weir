///|
/// Physical query planner lowering StreamingQuery AST into pipeline execution stages.
///
/// Converts declarative continuous SQL queries into executable filter, re-keying,
/// windowing, and projection operator chains.

///|
/// Compiled physical plan representation for a streaming SQL continuous query.
pub struct PhysicalStreamingPlan {
  source_name : String
  has_filter : Bool
  has_keyby : Bool
  has_window : Bool
  plan_summary : String
}

///|
/// Evaluates an AST SqlExpr against an incoming stream record.
pub fn evaluate_expr(expr : SqlExpr, record : @types.StreamRecord) -> @types.EventPayload {
  match expr {
    ColRef(name) => {
      if name == "$key" {
        @types.EventPayload::string(record.get_key())
      } else if name == "$timestamp" {
        @types.EventPayload::int(record.get_timestamp().to_millis())
      } else {
        match record.get_payload().get_field(name) {
          Some(v) => v
          None => @types.EventPayload::null()
        }
      }
    }
    LiteralDouble(d) => @types.EventPayload::double(d)
    LiteralInt(i) => @types.EventPayload::int(i)
    LiteralString(s) => @types.EventPayload::string(s)
    BinaryOp(op, left, right) => {
      let l_val = evaluate_expr(left, record)
      let r_val = evaluate_expr(right, record)

      match (l_val, r_val) {
        (@types.EventPayload::DoublePayload(d1), @types.EventPayload::DoublePayload(d2)) => {
          if op == ">" {
            @types.EventPayload::string(if d1 > d2 { "true" } else { "false" })
          } else if op == "<" {
            @types.EventPayload::string(if d1 < d2 { "true" } else { "false" })
          } else if op == "=" {
            @types.EventPayload::string(if d1 == d2 { "true" } else { "false" })
          } else if op == "+" {
            @types.EventPayload::double(d1 + d2)
          } else if op == "-" {
            @types.EventPayload::double(d1 - d2)
          } else {
            @types.EventPayload::null()
          }
        }
        (@types.EventPayload::IntPayload(i1), @types.EventPayload::IntPayload(i2)) => {
          if op == ">" {
            @types.EventPayload::string(if i1 > i2 { "true" } else { "false" })
          } else if op == "<" {
            @types.EventPayload::string(if i1 < i2 { "true" } else { "false" })
          } else if op == "=" {
            @types.EventPayload::string(if i1 == i2 { "true" } else { "false" })
          } else if op == "+" {
            @types.EventPayload::int(i1 + i2)
          } else if op == "-" {
            @types.EventPayload::int(i1 - i2)
          } else {
            @types.EventPayload::null()
          }
        }
        _ => @types.EventPayload::null()
      }
    }
    FuncCall(func_name, _args) => {
      @types.EventPayload::string("fn:" + func_name)
    }
  }
}

///|
/// Compiles a StreamingQuery AST into a PhysicalStreamingPlan description.
pub fn compile_query_plan(query : StreamingQuery) -> PhysicalStreamingPlan {
  let source_name = query.get_source_stream()
  let has_filter = query.get_where_clause() is Some(_)
  let has_keyby = query.get_group_by_keys().length() > 0
  let has_window = query.get_window() is Some(_)

  let sb = StringBuilder()
  sb.write_string("Plan: Source(" + source_name + ")")
  if has_filter {
    sb.write_string(" -> Filter")
  }
  if has_keyby {
    sb.write_string(" -> KeyBy")
  }
  if has_window {
    sb.write_string(" -> WindowAggregate")
  }
  sb.write_string(" -> Project")

  {
    source_name,
    has_filter,
    has_keyby,
    has_window,
    plan_summary: sb.to_string(),
  }
}
