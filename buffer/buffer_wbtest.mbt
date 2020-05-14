///|
/// Comprehensive whitebox test suite for RingBuffer, BoundedChannel, and SPSCQueue.

///|
test "ring buffer power of two and capacity" {
  let ring : RingBuffer[Int] = RingBuffer::new(5)
  assert_eq(ring.capacity(), 8)
  assert_true(ring.is_empty())
  assert_false(ring.is_full())
  assert_eq(ring.length(), 0)
}

///|
test "ring buffer fifo wraparound" {
  let ring : RingBuffer[Int] = RingBuffer::new(4)
  assert_eq(ring.capacity(), 4)

  assert_true(ring.push(10))
  assert_true(ring.push(20))
  assert_true(ring.push(30))
  assert_true(ring.push(40))
  assert_true(ring.is_full())
  assert_false(ring.push(50))

  assert_eq(ring.peek(), Some(10))
  assert_eq(ring.pop(), Some(10))
  assert_eq(ring.pop(), Some(20))
  assert_false(ring.is_full())
  assert_eq(ring.length(), 2)

  // Wraparound push
  assert_true(ring.push(60))
  assert_true(ring.push(70))
  assert_true(ring.is_full())

  assert_eq(ring.pop(), Some(30))
  assert_eq(ring.pop(), Some(40))
  assert_eq(ring.pop(), Some(60))
  assert_eq(ring.pop(), Some(70))
  assert_true(ring.is_empty())
  assert_eq(ring.pop(), None)
}

///|
test "ring buffer to_array and clear" {
  let ring : RingBuffer[String] = RingBuffer::new(4)
  let _ = ring.push("alpha")
  let _ = ring.push("beta")
  let _ = ring.push("gamma")

  let arr = ring.to_array()
  assert_eq(arr.length(), 3)
  assert_eq(arr[0], "alpha")
  assert_eq(arr[1], "beta")
  assert_eq(arr[2], "gamma")

  ring.clear()
  assert_true(ring.is_empty())
  assert_eq(ring.length(), 0)
}

///|
test "bounded channel block policy" {
  let chan : BoundedChannel[Int] = BoundedChannel::new(4, OverflowPolicy::block())
  assert_eq(chan.capacity(), 4)

  assert_true(chan.push(1))
  assert_true(chan.push(2))
  assert_true(chan.push(3))
  assert_true(chan.push(4))
  assert_false(chan.push(5))

  let stats = chan.get_stats()
  assert_eq(stats.pushed, 4L)
  assert_eq(stats.dropped, 1L)
  assert_eq(stats.peak_occupancy, 4)
}

///|
test "bounded channel drop oldest policy" {
  let chan : BoundedChannel[Int] = BoundedChannel::new(4, OverflowPolicy::drop_oldest())
  assert_true(chan.push(1))
  assert_true(chan.push(2))
  assert_true(chan.push(3))
  assert_true(chan.push(4))

  // 1 is dropped, 5 is admitted
  assert_true(chan.push(5))
  assert_eq(chan.pop(), Some(2))
  assert_eq(chan.pop(), Some(3))
  assert_eq(chan.pop(), Some(4))
  assert_eq(chan.pop(), Some(5))
  assert_true(chan.is_empty())

  let stats = chan.get_stats()
  assert_eq(stats.dropped, 1L)
}

///|
test "bounded channel drop newest policy" {
  let chan : BoundedChannel[Int] = BoundedChannel::new(4, OverflowPolicy::drop_newest())
  assert_true(chan.push(1))
  assert_true(chan.push(2))
  assert_true(chan.push(3))
  assert_true(chan.push(4))

  // 5 is dropped
  assert_false(chan.push(5))
  assert_eq(chan.pop(), Some(1))

  let stats = chan.get_stats()
  assert_eq(stats.dropped, 1L)
}

///|
test "bounded channel batch operations and draining" {
  let chan : BoundedChannel[Int] = BoundedChannel::new(8, OverflowPolicy::block())
  let items = [10, 20, 30, 40, 50]
  assert_eq(chan.push_batch(items), 5)
  assert_eq(chan.length(), 5)

  let popped = chan.pop_batch(3)
  assert_eq(popped.length(), 3)
  assert_eq(popped[0], 10)
  assert_eq(popped[1], 20)
  assert_eq(popped[2], 30)

  let drained = chan.drain()
  assert_eq(drained.length(), 2)
  assert_eq(drained[0], 40)
  assert_eq(drained[1], 50)
  assert_true(chan.is_empty())
}

///|
test "spsc queue basic and batch operations" {
  let queue : SPSCQueue[String] = SPSCQueue::new("stage-1-2", 4)
  assert_eq(queue.get_name(), "stage-1-2")
  assert_eq(queue.capacity(), 4)

  assert_true(queue.enqueue("a"))
  assert_true(queue.enqueue("b"))
  assert_eq(queue.length(), 2)
  assert_eq(queue.peek(), Some("a"))
  assert_eq(queue.dequeue(), Some("a"))
  assert_eq(queue.dequeue(), Some("b"))
  assert_eq(queue.dequeue(), None)

  let count = queue.batch_enqueue(["x", "y", "z"])
  assert_eq(count, 3)
  let batch = queue.batch_dequeue(2)
  assert_eq(batch.length(), 2)
  assert_eq(batch[0], "x")
  assert_eq(batch[1], "y")
  assert_eq(queue.length(), 1)
}
