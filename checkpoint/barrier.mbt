///|
/// Chandy-Lamport barrier alignment algorithm for distributed stateful operators.
///
/// Multi-input operators synchronize on checkpoint barriers by buffering channels
/// that have already seen the barrier while waiting for trailing channels.

///|
/// Action returned by the barrier alignment coordinator on record arrival.
pub enum AlignmentDecision {
  ProcessNow(@types.StreamRecord)
  BufferChannel(@types.StreamRecord)
  AlignmentComplete(Int64, Array[@types.StreamRecord])
}

///|
/// Tracks barrier arrival across upstream channels and buffers post-barrier records.
pub struct BarrierAlignmentTracker {
  num_channels : Int
  received_barrier : FixedArray[Bool]
  alignment_buffers : FixedArray[@buffer.BoundedChannel[@types.StreamRecord]]
  mut active_checkpoint_id : Int64?
  mut barriers_received : Int
}

///|
/// Creates a BarrierAlignmentTracker for given channel count and buffer capacity.
pub fn BarrierAlignmentTracker::new(
  num_channels : Int,
  buffer_capacity : Int,
) -> BarrierAlignmentTracker {
  let flags = FixedArray::make(num_channels, false)
  let buffers : FixedArray[@buffer.BoundedChannel[@types.StreamRecord]] = FixedArray::make(
    num_channels,
    @buffer.BoundedChannel::new(
      buffer_capacity,
      @buffer.OverflowPolicy::drop_oldest(),
    ),
  )
  for i = 0; i < num_channels; i = i + 1 {
    buffers[i] = @buffer.BoundedChannel::new(
      buffer_capacity,
      @buffer.OverflowPolicy::drop_oldest(),
    )
  }

  {
    num_channels,
    received_barrier: flags,
    alignment_buffers: buffers,
    active_checkpoint_id: None,
    barriers_received: 0,
  }
}

///|
/// Evaluates an incoming stream record on a specific input channel.
pub fn BarrierAlignmentTracker::on_record(
  self : BarrierAlignmentTracker,
  channel_idx : Int,
  record : @types.StreamRecord,
) -> AlignmentDecision {
  if channel_idx < 0 || channel_idx >= self.num_channels {
    return ProcessNow(record)
  }

  if record.is_barrier() {
    let barrier_id = match record.get_payload() {
      @types.EventPayload::IntPayload(id) => id
      _ => 0L
    }

    match self.active_checkpoint_id {
      None => {
        self.active_checkpoint_id = Some(barrier_id)
        self.received_barrier[channel_idx] = true
        self.barriers_received = 1
      }
      Some(cur_id) => {
        if barrier_id == cur_id && !self.received_barrier[channel_idx] {
          self.received_barrier[channel_idx] = true
          self.barriers_received = self.barriers_received + 1
        }
      }
    }

    // Check if all channels have now aligned
    if self.barriers_received >= self.num_channels {
      let aligned_id = match self.active_checkpoint_id {
        Some(id) => id
        None => 0L
      }

      // Collect all buffered events in FIFO order
      let buffered_events : Array[@types.StreamRecord] = []
      for i = 0; i < self.num_channels; i = i + 1 {
        let drained = self.alignment_buffers[i].drain()
        for j = 0; j < drained.length(); j = j + 1 {
          buffered_events.push(drained[j])
        }
        self.received_barrier[i] = false
      }

      self.active_checkpoint_id = None
      self.barriers_received = 0

      AlignmentComplete(aligned_id, buffered_events)
    } else {
      // Barrier absorbed, channel now aligned and waiting for other channels
      BufferChannel(record)
    }
  } else {
    // Normal data record
    if self.received_barrier[channel_idx] {
      // Post-barrier event: must buffer until alignment finishes!
      let _ = self.alignment_buffers[channel_idx].push(record)
      BufferChannel(record)
    } else {
      // Pre-barrier event: process immediately
      ProcessNow(record)
    }
  }
}

///|
/// Returns true if currently in an active alignment phase.
pub fn BarrierAlignmentTracker::is_aligning(
  self : BarrierAlignmentTracker,
) -> Bool {
  self.active_checkpoint_id is Some(_)
}

///|
/// Returns total number of aligned channels for the active checkpoint.
pub fn BarrierAlignmentTracker::get_aligned_channels_count(
  self : BarrierAlignmentTracker,
) -> Int {
  self.barriers_received
}
