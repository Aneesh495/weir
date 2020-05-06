///|
/// Bounded flow-controlled streaming channel with configurable overflow policy.
///
/// Serves as the primary communication and buffering pipe between pipeline operators.

///|
/// Bounded event channel wrapping a ring buffer and saturation handler.
pub struct BoundedChannel[T] {
  ring : RingBuffer[T]
  policy : OverflowPolicy
  stats : ChannelStats
  mut closed : Bool
}

///|
/// Creates a new BoundedChannel with specified capacity and overflow policy.
pub fn[T] BoundedChannel::new(
  capacity : Int,
  policy : OverflowPolicy,
) -> BoundedChannel[T] {
  {
    ring: RingBuffer::new(capacity),
    policy,
    stats: ChannelStats::new(),
    closed: false,
  }
}

///|
/// Pushes an item into the channel following the configured OverflowPolicy.
/// Returns true if the item was admitted, or false if rejected.
pub fn[T] BoundedChannel::push(self : BoundedChannel[T], item : T) -> Bool {
  if self.closed {
    false
  } else if !self.ring.is_full() {
    let ok = self.ring.push(item)
    if ok {
      self.stats.record_push(self.ring.length())
    }
    ok
  } else {
    match self.policy {
      Block => {
        self.stats.record_drop()
        false
      }
      DropNewest => {
        self.stats.record_drop()
        false
      }
      DropOldest => {
        let _ = self.ring.pop()
        self.stats.record_drop()
        let ok = self.ring.push(item)
        if ok {
          self.stats.record_push(self.ring.length())
        }
        ok
      }
    }
  }
}

///|
/// Pops an item from the channel.
pub fn[T] BoundedChannel::pop(self : BoundedChannel[T]) -> T? {
  let item = self.ring.pop()
  match item {
    Some(_) => self.stats.record_pop()
    None => ()
  }
  item
}

///|
/// Peeks at the next item in the channel without removing it.
pub fn[T] BoundedChannel::peek(self : BoundedChannel[T]) -> T? {
  self.ring.peek()
}

///|
/// Returns the current number of elements in the channel.
pub fn[T] BoundedChannel::length(self : BoundedChannel[T]) -> Int {
  self.ring.length()
}

///|
/// Returns the capacity of the underlying ring buffer.
pub fn[T] BoundedChannel::capacity(self : BoundedChannel[T]) -> Int {
  self.ring.capacity()
}

///|
/// Returns true if the channel has no buffered items.
pub fn[T] BoundedChannel::is_empty(self : BoundedChannel[T]) -> Bool {
  self.ring.is_empty()
}

///|
/// Returns true if the channel is full.
pub fn[T] BoundedChannel::is_full(self : BoundedChannel[T]) -> Bool {
  self.ring.is_full()
}

///|
/// Closes the channel to prevent further writes.
pub fn[T] BoundedChannel::close(self : BoundedChannel[T]) -> Unit {
  self.closed = true
}

///|
/// Returns true if the channel is closed.
pub fn[T] BoundedChannel::is_closed(self : BoundedChannel[T]) -> Bool {
  self.closed
}

///|
/// Drains all available elements into an Array in FIFO order.
pub fn[T] BoundedChannel::drain(self : BoundedChannel[T]) -> Array[T] {
  let result = []
  while true {
    match self.pop() {
      Some(item) => result.push(item)
      None => break
    }
  }
  result
}

///|
/// Pushes a batch of items into the channel. Returns count admitted.
pub fn[T] BoundedChannel::push_batch(
  self : BoundedChannel[T],
  items : Array[T],
) -> Int {
  let mut admitted = 0
  for i = 0; i < items.length(); i = i + 1 {
    if self.push(items[i]) {
      admitted = admitted + 1
    }
  }
  admitted
}

///|
/// Pops up to max_items from the channel into an Array.
pub fn[T] BoundedChannel::pop_batch(
  self : BoundedChannel[T],
  max_items : Int,
) -> Array[T] {
  let result = []
  let mut count = 0
  while count < max_items {
    match self.pop() {
      Some(item) => {
        result.push(item)
        count = count + 1
      }
      None => break
    }
  }
  result
}

///|
/// Returns telemetry statistics for the channel.
pub fn[T] BoundedChannel::get_stats(self : BoundedChannel[T]) -> ChannelStats {
  self.stats
}
