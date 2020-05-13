///|
/// Single-Producer Single-Consumer (SPSC) inter-operator handoff queue.
///
/// Provides a dedicated lock-free conduit for streaming records between
/// adjacent stages in an execution DAG.

///|
/// SPSC queue tailored for pipeline operator decoupling.
pub struct SPSCQueue[T] {
  name : String
  ring : RingBuffer[T]
}

///|
/// Creates an SPSC queue with given name and capacity.
pub fn[T] SPSCQueue::new(name : String, capacity : Int) -> SPSCQueue[T] {
  { name, ring: RingBuffer::new(capacity) }
}

///|
/// Returns the queue name.
pub fn[T] SPSCQueue::get_name(self : SPSCQueue[T]) -> String {
  self.name
}

///|
/// Enqueues an item if capacity allows.
pub fn[T] SPSCQueue::enqueue(self : SPSCQueue[T], item : T) -> Bool {
  self.ring.push(item)
}

///|
/// Dequeues the next item.
pub fn[T] SPSCQueue::dequeue(self : SPSCQueue[T]) -> T? {
  self.ring.pop()
}

///|
/// Peeks at the head item without removal.
pub fn[T] SPSCQueue::peek(self : SPSCQueue[T]) -> T? {
  self.ring.peek()
}

///|
/// Current queue length.
pub fn[T] SPSCQueue::length(self : SPSCQueue[T]) -> Int {
  self.ring.length()
}

///|
/// Queue buffer capacity.
pub fn[T] SPSCQueue::capacity(self : SPSCQueue[T]) -> Int {
  self.ring.capacity()
}

///|
/// Checks if queue is empty.
pub fn[T] SPSCQueue::is_empty(self : SPSCQueue[T]) -> Bool {
  self.ring.is_empty()
}

///|
/// Checks if queue is full.
pub fn[T] SPSCQueue::is_full(self : SPSCQueue[T]) -> Bool {
  self.ring.is_full()
}

///|
/// Enqueues multiple items in order. Returns count admitted.
pub fn[T] SPSCQueue::batch_enqueue(self : SPSCQueue[T], items : Array[T]) -> Int {
  let mut count = 0
  for i = 0; i < items.length(); i = i + 1 {
    if self.enqueue(items[i]) {
      count = count + 1
    } else {
      break
    }
  }
  count
}

///|
/// Dequeues up to max items into an Array.
pub fn[T] SPSCQueue::batch_dequeue(self : SPSCQueue[T], max : Int) -> Array[T] {
  let result = []
  let mut count = 0
  while count < max {
    match self.dequeue() {
      Some(item) => {
        result.push(item)
        count = count + 1
      }
      None => break
    }
  }
  result
}
