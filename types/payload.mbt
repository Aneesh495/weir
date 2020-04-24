///|
/// Dynamic event payload primitives for unstructured and semi-structured streams.
///
/// Supports null, integer, floating-point, string, binary byte array,
/// list, and nested key-value dictionary payloads.

///|
/// Polymorphic value representation for streaming events.
pub enum EventPayload {
  NullPayload
  IntPayload(Int64)
  DoublePayload(Double)
  StringPayload(String)
  BytesPayload(Bytes)
  ArrayPayload(Array[EventPayload])
  MapPayload(Array[(String, EventPayload)])
}

///|
pub impl Eq for EventPayload with equal(self, other) {
  match (self, other) {
    (NullPayload, NullPayload) => true
    (IntPayload(a), IntPayload(b)) => a == b
    (DoublePayload(a), DoublePayload(b)) => a == b
    (StringPayload(a), StringPayload(b)) => a == b
    (BytesPayload(a), BytesPayload(b)) => a == b
    (ArrayPayload(a), ArrayPayload(b)) => {
      if a.length() != b.length() {
        false
      } else {
        let mut same = true
        for i = 0; i < a.length(); i = i + 1 {
          if a[i] != b[i] {
            same = false
            break
          }
        }
        same
      }
    }
    (MapPayload(a), MapPayload(b)) => {
      if a.length() != b.length() {
        false
      } else {
        let mut same = true
        for i = 0; i < a.length(); i = i + 1 {
          let (k_a, v_a) = a[i]
          let (k_b, v_b) = b[i]
          if k_a != k_b || v_a != v_b {
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
pub impl Eq for EventPayload with not_equal(self, other) {
  !(self == other)
}

///|
pub extend EventPayload with Eq::{equal, not_equal}

///|
pub impl Show for EventPayload with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for EventPayload with to_string(self) {
  match self {
    NullPayload => "null"
    IntPayload(i) => i.to_string()
    DoublePayload(d) => d.to_string()
    StringPayload(s) => "\"" + s + "\""
    BytesPayload(b) => "<bytes len=" + b.length().to_string() + ">"
    ArrayPayload(arr) => {
      let sb = StringBuilder()
      sb.write_string("[")
      for i = 0; i < arr.length(); i = i + 1 {
        if i > 0 {
          sb.write_string(", ")
        }
        sb.write_string(arr[i].to_string())
      }
      sb.write_string("]")
      sb.to_string()
    }
    MapPayload(entries) => {
      let sb = StringBuilder()
      sb.write_string("{")
      for i = 0; i < entries.length(); i = i + 1 {
        if i > 0 {
          sb.write_string(", ")
        }
        let (k, v) = entries[i]
        sb.write_string("\"" + k + "\": " + v.to_string())
      }
      sb.write_string("}")
      sb.to_string()
    }
  }
}

///|
pub extend EventPayload with Show::{output, to_string}

///|
/// Attempts to extract an Int64 value from the payload.
pub fn EventPayload::as_int(self : EventPayload) -> Int64? {
  match self {
    IntPayload(i) => Some(i)
    _ => None
  }
}

///|
/// Attempts to extract a Double value from the payload.
pub fn EventPayload::as_double(self : EventPayload) -> Double? {
  match self {
    DoublePayload(d) => Some(d)
    IntPayload(i) => Some(i.to_double())
    _ => None
  }
}

///|
/// Attempts to extract a String value from the payload.
pub fn EventPayload::as_string(self : EventPayload) -> String? {
  match self {
    StringPayload(s) => Some(s)
    _ => None
  }
}

///|
/// Attempts to extract an array from the payload.
pub fn EventPayload::as_array(self : EventPayload) -> Array[EventPayload]? {
  match self {
    ArrayPayload(arr) => Some(arr)
    _ => None
  }
}

///|
/// Look up a field value by key in a MapPayload.
pub fn EventPayload::get_field(self : EventPayload, key : String) -> EventPayload? {
  match self {
    MapPayload(entries) => {
      let mut found : EventPayload? = None
      for i = 0; i < entries.length(); i = i + 1 {
        let (k, v) = entries[i]
        if k == key {
          found = Some(v)
          break
        }
      }
      found
    }
    _ => None
  }
}

///|
/// Look up an index in an ArrayPayload.
pub fn EventPayload::get_index(self : EventPayload, index : Int) -> EventPayload? {
  match self {
    ArrayPayload(arr) => {
      if index >= 0 && index < arr.length() {
        Some(arr[index])
      } else {
        None
      }
    }
    _ => None
  }
}
///|
/// Creates a null payload.
pub fn EventPayload::null() -> EventPayload {
  NullPayload
}

///|
/// Creates an integer payload.
pub fn EventPayload::int(i : Int64) -> EventPayload {
  IntPayload(i)
}

///|
/// Creates a floating-point double payload.
pub fn EventPayload::double(d : Double) -> EventPayload {
  DoublePayload(d)
}

///|
/// Creates a string payload.
pub fn EventPayload::string(s : String) -> EventPayload {
  StringPayload(s)
}

///|
/// Creates a binary bytes payload.
pub fn EventPayload::bytes(b : Bytes) -> EventPayload {
  BytesPayload(b)
}

///|
/// Creates an array payload.
pub fn EventPayload::array(arr : Array[EventPayload]) -> EventPayload {
  ArrayPayload(arr)
}

///|
/// Creates a key-value dictionary payload.
pub fn EventPayload::map(entries : Array[(String, EventPayload)]) -> EventPayload {
  MapPayload(entries)
}
