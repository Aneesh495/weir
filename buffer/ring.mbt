///|
/// High-throughput circular ring buffer for bounded stream queueing.
///
/// Implements power-of-two capacity sizing and bitwise masking for fast
/// modular index calculations without integer division.

///|
/// Power-of-two bounded circular ring buffer.
pub struct RingBuffer[T] {
  slots : FixedArray[T?]
  capacity : Int
  mask : Int
  mut head : Int
  mut tail : Int
  mut size : Int
}

///|
/// Computes the smallest power of two greater than or equal to n, clamped to min 2.
fn next_power_of_two(n : Int) -> Int {
  let mut p = 2
  while p < n && p < 1073741824 {
    p = p * 2
  }
  p
}

///|
/// Creates a new RingBuffer with capacity rounded up to the nearest power of two.
pub fn[T] RingBuffer::new(min_capacity : Int) -> RingBuffer[T] {
  let cap = next_power_of_two(min_capacity)
  {
    slots: FixedArray::make(cap, None),
    capacity: cap,
    mask: cap - 1,
    head: 0,
    tail: 0,
    size: 0,
  }
}

///|
/// Returns true if the buffer has no elements.
pub fn[T] RingBuffer::is_empty(self : RingBuffer[T]) -> Bool {
  self.size == 0
}

///|
/// Returns true if the buffer has reached maximum capacity.
pub fn[T] RingBuffer::is_full(self : RingBuffer[T]) -> Bool {
  self.size == self.capacity
}

///|
/// Returns the current number of elements queued in the buffer.
pub fn[T] RingBuffer::length(self : RingBuffer[T]) -> Int {
  self.size
}

///|
/// Returns the total capacity of the ring buffer.
pub fn[T] RingBuffer::capacity(self : RingBuffer[T]) -> Int {
  self.capacity
}

///|
/// Attempts to push an element to the tail of the buffer.
/// Returns true on success, or false if the buffer is full.
pub fn[T] RingBuffer::push(self : RingBuffer[T], item : T) -> Bool {
  if self.is_full() {
    false
  } else {
    self.slots[self.tail] = Some(item)
    self.tail = (self.tail + 1) & self.mask
    self.size = self.size + 1
    true
  }
}

///|
/// Pops an element from the head of the buffer.
/// Returns Some(item) or None if the buffer is empty.
pub fn[T] RingBuffer::pop(self : RingBuffer[T]) -> T? {
  if self.is_empty() {
    None
  } else {
    let item = self.slots[self.head]
    self.slots[self.head] = None
    self.head = (self.head + 1) & self.mask
    self.size = self.size - 1
    item
  }
}

///|
/// Inspects the element at the head of the buffer without removing it.
pub fn[T] RingBuffer::peek(self : RingBuffer[T]) -> T? {
  if self.is_empty() {
    None
  } else {
    self.slots[self.head]
  }
}

///|
/// Clears all elements from the ring buffer.
pub fn[T] RingBuffer::clear(self : RingBuffer[T]) -> Unit {
  while !self.is_empty() {
    let _ = self.pop()
  }
  self.head = 0
  self.tail = 0
  self.size = 0
}

///|
/// Copies all currently buffered elements into a new Array in FIFO order.
pub fn[T] RingBuffer::to_array(self : RingBuffer[T]) -> Array[T] {
  let result = []
  let mut idx = self.head
  for i = 0; i < self.size; i = i + 1 {
    match self.slots[idx] {
      Some(item) => result.push(item)
      None => ()
    }
    idx = (idx + 1) & self.mask
  }
  result
}
