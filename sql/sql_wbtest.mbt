///|
/// Comprehensive tests for WeirSQL lexer, parser, expression evaluator, and query planner.

///|
test "weir sql lexer tokenizes streaming queries" {
  let sql = "SELECT host, cpu FROM metrics WHERE cpu > 80.5"
  let tokens = scan_sql(sql)

  assert_true(tokens[0].kind == KwSelect)
  assert_true(tokens[1].kind == Ident("host"))
  assert_true(tokens[2].kind == Comma)
  assert_true(tokens[3].kind == Ident("cpu"))
  assert_true(tokens[4].kind == KwFrom)
  assert_true(tokens[5].kind == Ident("metrics"))
  assert_true(tokens[6].kind == KwWhere)
  assert_true(tokens[7].kind == Ident("cpu"))
  assert_true(tokens[8].kind == OpGt)
  assert_true(tokens[9].kind == DblLit(80.5))
}

///|
test "weir sql parser parses continuous streaming query" {
  let sql = "SELECT device_id, temperature FROM telemetry WINDOW TUMBLE(ts, INTERVAL 10 SECOND) WHERE temperature > 75.0 GROUP BY device_id EMIT CHANGES"
  let tokens = scan_sql(sql)
  let parser = SqlParser::new(tokens)

  let query_opt = parser.parse()
  assert_true(query_opt is Some(_))

  let q = match query_opt {
    Some(res) => res
    None => abort("Failed to parse query")
  }

  assert_eq(q.get_source_stream(), "telemetry")
  assert_eq(q.get_select_fields().length(), 2)
  assert_true(q.get_window() == Some(WindowSpec::Tumble("ts", 10L)))
  assert_true(q.get_where_clause() is Some(_))
  assert_eq(q.get_group_by_keys().length(), 1)
  assert_eq(q.get_group_by_keys()[0], "device_id")
}

///|
test "expression evaluation over stream records" {
  let ts = @types.Timestamp::from_seconds(100L)
  let payload = @types.EventPayload::map([
    ("speed", @types.EventPayload::double(65.0)),
    ("limit", @types.EventPayload::double(55.0)),
  ])
  let record = @types.StreamRecord::data("truck_1", ts, payload)

  let expr_col = SqlExpr::ColRef("speed")
  assert_true(evaluate_expr(expr_col, record) == @types.EventPayload::double(65.0))

  let expr_gt = SqlExpr::BinaryOp(">", SqlExpr::ColRef("speed"), SqlExpr::ColRef("limit"))
  assert_true(evaluate_expr(expr_gt, record) == @types.EventPayload::string("true"))
}

///|
test "query planner compiles physical streaming plan" {
  let sql = "SELECT symbol, price FROM trades WINDOW TUMBLE(ts, INTERVAL 5 SECOND) WHERE price > 100.0 GROUP BY symbol"
  let tokens = scan_sql(sql)
  let q = match SqlParser::new(tokens).parse() {
    Some(res) => res
    None => abort("Failed to parse")
  }

  let plan = compile_query_plan(q)
  assert_eq(plan.source_name, "trades")
  assert_true(plan.has_filter)
  assert_true(plan.has_keyby)
  assert_true(plan.has_window)
  assert_eq(plan.plan_summary, "Plan: Source(trades) -> Filter -> KeyBy -> WindowAggregate -> Project")
}
