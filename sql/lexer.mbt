///|
/// Lexical scanner for continuous streaming SQL queries.
///
/// Converts raw query text into a structured token stream with support for
/// keywords, identifiers, numeric literals, and quoted strings.

///|
/// Helper to convert a code unit to uppercase ASCII Char.
fn to_upper_code(code : Int) -> Char {
  if code >= 97 && code <= 122 {
    (code - 32).unsafe_to_char()
  } else {
    code.unsafe_to_char()
  }
}

///|
/// Checks if code unit is ASCII whitespace.
fn is_whitespace(code : Int) -> Bool {
  code == 32 || code == 9 || code == 10 || code == 13
}

///|
/// Checks if code unit is ASCII alphabetic or underscore.
fn is_alpha(code : Int) -> Bool {
  (code >= 65 && code <= 90) || (code >= 97 && code <= 122) || code == 95
}

///|
/// Checks if code unit is ASCII digit.
fn is_digit(code : Int) -> Bool {
  code >= 48 && code <= 57
}

///|
/// Converts a string to uppercase ASCII.
fn to_upper_string(s : String) -> String {
  let sb = StringBuilder()
  for i = 0; i < s.length(); i = i + 1 {
    sb.write_char(to_upper_code(s[i].to_int()))
  }
  sb.to_string()
}

///|
/// Matches an identifier string against SQL keywords.
fn match_keyword(upper : String) -> TokenKind? {
  match upper {
    "SELECT" => Some(KwSelect)
    "FROM" => Some(KwFrom)
    "WHERE" => Some(KwWhere)
    "GROUP" => Some(KwGroup)
    "BY" => Some(KwBy)
    "WINDOW" => Some(KwWindow)
    "TUMBLE" => Some(KwTumble)
    "HOP" => Some(KwHop)
    "SESSION" => Some(KwSession)
    "INTERVAL" => Some(KwInterval)
    "SECOND" => Some(KwSecond)
    "MINUTE" => Some(KwMinute)
    "HOUR" => Some(KwHour)
    "EMIT" => Some(KwEmit)
    "CHANGES" => Some(KwChanges)
    "WATERMARK" => Some(KwWatermark)
    "AND" => Some(KwAnd)
    "OR" => Some(KwOr)
    "NOT" => Some(KwNot)
    "AS" => Some(KwAs)
    _ => None
  }
}

///|
/// Scans a full SQL string into an Array of Tokens.
pub fn scan_sql(input : String) -> Array[Token] {
  let tokens : Array[Token] = []
  let len = input.length()
  let mut i = 0

  while i < len {
    let code = input[i].to_int()

    if is_whitespace(code) {
      i = i + 1
      continue
    }

    let start_pos = i

    // Symbols & Punctuation: ',' = 44, '.' = 46, '*' = 42, '(' = 40, ')' = 41
    if code == 44 {
      tokens.push(Token::new(Comma, start_pos))
      i = i + 1
      continue
    }
    if code == 46 {
      tokens.push(Token::new(Dot, start_pos))
      i = i + 1
      continue
    }
    if code == 42 {
      tokens.push(Token::new(Star, start_pos))
      i = i + 1
      continue
    }
    if code == 40 {
      tokens.push(Token::new(LParen, start_pos))
      i = i + 1
      continue
    }
    if code == 41 {
      tokens.push(Token::new(RParen, start_pos))
      i = i + 1
      continue
    }
    if code == 43 {
      tokens.push(Token::new(OpPlus, start_pos))
      i = i + 1
      continue
    }
    if code == 45 {
      tokens.push(Token::new(OpMinus, start_pos))
      i = i + 1
      continue
    }
    if code == 61 {
      tokens.push(Token::new(OpEq, start_pos))
      i = i + 1
      continue
    }
    if code == 33 && i + 1 < len && input[i + 1].to_int() == 61 {
      tokens.push(Token::new(OpNeq, start_pos))
      i = i + 2
      continue
    }
    if code == 62 {
      if i + 1 < len && input[i + 1].to_int() == 61 {
        tokens.push(Token::new(OpGte, start_pos))
        i = i + 2
      } else {
        tokens.push(Token::new(OpGt, start_pos))
        i = i + 1
      }
      continue
    }
    if code == 60 {
      if i + 1 < len && input[i + 1].to_int() == 61 {
        tokens.push(Token::new(OpLte, start_pos))
        i = i + 2
      } else {
        tokens.push(Token::new(OpLt, start_pos))
        i = i + 1
      }
      continue
    }

    // Quoted String Literal: single quote = 39, double quote = 34
    if code == 39 || code == 34 {
      let quote = code
      i = i + 1
      let sb = StringBuilder()
      while i < len && input[i].to_int() != quote {
        sb.write_char(input[i].to_int().unsafe_to_char())
        i = i + 1
      }
      if i < len && input[i].to_int() == quote {
        i = i + 1
      }
      tokens.push(Token::new(StrLit(sb.to_string()), start_pos))
      continue
    }

    // Numeric Literal (Integer or Double)
    if is_digit(code) {
      let mut num_str = ""
      let mut is_dbl = false
      while i < len && (is_digit(input[i].to_int()) || input[i].to_int() == 46) {
        if input[i].to_int() == 46 {
          is_dbl = true
        }
        num_str = num_str + input[i].to_int().unsafe_to_char().to_string()
        i = i + 1
      }

      if is_dbl {
        let mut d_val = 0.0
        let mut divisor = 1.0
        let mut seen_dot = false
        for idx = 0; idx < num_str.length(); idx = idx + 1 {
          let ch_code = num_str[idx].to_int()
          if ch_code == 46 {
            seen_dot = true
          } else {
            let digit = (ch_code - 48).to_double()
            if !seen_dot {
              d_val = d_val * 10.0 + digit
            } else {
              divisor = divisor * 10.0
              d_val = d_val + digit / divisor
            }
          }
        }
        tokens.push(Token::new(DblLit(d_val), start_pos))
      } else {
        let mut i_val = 0L
        for idx = 0; idx < num_str.length(); idx = idx + 1 {
          let digit = (num_str[idx].to_int() - 48).to_int64()
          i_val = i_val * 10L + digit
        }
        tokens.push(Token::new(IntLit(i_val), start_pos))
      }
      continue
    }

    // Identifiers and Keywords
    if is_alpha(code) {
      let mut ident_str = ""
      while i < len && (is_alpha(input[i].to_int()) || is_digit(input[i].to_int())) {
        ident_str = ident_str + input[i].to_int().unsafe_to_char().to_string()
        i = i + 1
      }

      let upper = to_upper_string(ident_str)
      match match_keyword(upper) {
        Some(kw) => tokens.push(Token::new(kw, start_pos))
        None => tokens.push(Token::new(Ident(ident_str), start_pos))
      }
      continue
    }

    // Advance unhandled char
    i = i + 1
  }

  tokens.push(Token::new(Eof, len))
  tokens
}
