///|
/// Multi-partition watermark alignment across parallel stream channels.
///
/// Ensures that an operator with multiple upstream inputs only advances its
/// internal watermark to the minimum watermark across all active channels.

///|
/// Tracks and aligns watermarks across multiple upstream input channels.
pub struct PartitionWatermarkTracker {
  num_partitions : Int
  channel_watermarks : FixedArray[@types.Watermark]
  is_idle : FixedArray[Bool]
  mut operator_watermark : @types.Watermark
}

///|
/// Creates a PartitionWatermarkTracker for a fixed number of input partitions.
pub fn PartitionWatermarkTracker::new(
  num_partitions : Int,
) -> PartitionWatermarkTracker {
  let min_wm = @types.Watermark::from_timestamp(@types.Timestamp::min_value())
  let wms = FixedArray::make(num_partitions, min_wm)
  let idles = FixedArray::make(num_partitions, false)
  {
    num_partitions,
    channel_watermarks: wms,
    is_idle: idles,
    operator_watermark: min_wm,
  }
}

///|
/// Calculates the current minimum watermark across all active (non-idle) channels.
fn PartitionWatermarkTracker::calculate_min_active(
  self : PartitionWatermarkTracker,
) -> @types.Watermark {
  let mut min_wm = @types.Watermark::from_timestamp(@types.Timestamp::max_value())
  let mut has_active = false

  for i = 0; i < self.num_partitions; i = i + 1 {
    if !self.is_idle[i] {
      has_active = true
      if self.channel_watermarks[i] < min_wm {
        min_wm = self.channel_watermarks[i]
      }
    }
  }

  if has_active {
    min_wm
  } else {
    self.operator_watermark
  }
}

///|
/// Advances the watermark for a specific partition.
/// Returns Some(new_operator_watermark) if the global operator watermark advanced.
pub fn PartitionWatermarkTracker::advance_partition(
  self : PartitionWatermarkTracker,
  partition_id : Int,
  watermark : @types.Watermark,
) -> @types.Watermark? {
  if partition_id < 0 || partition_id >= self.num_partitions {
    None
  } else {
    if watermark > self.channel_watermarks[partition_id] {
      self.channel_watermarks[partition_id] = watermark
      self.is_idle[partition_id] = false
      let new_min = self.calculate_min_active()
      if new_min > self.operator_watermark {
        self.operator_watermark = new_min
        Some(new_min)
      } else {
        None
      }
    } else {
      None
    }
  }
}

///|
/// Marks a partition as temporarily idle, excluding it from minimum alignment.
pub fn PartitionWatermarkTracker::mark_idle(
  self : PartitionWatermarkTracker,
  partition_id : Int,
) -> @types.Watermark? {
  if partition_id >= 0 && partition_id < self.num_partitions {
    self.is_idle[partition_id] = true
    let new_min = self.calculate_min_active()
    if new_min > self.operator_watermark {
      self.operator_watermark = new_min
      Some(new_min)
    } else {
      None
    }
  } else {
    None
  }
}

///|
/// Marks an idle partition as active again.
pub fn PartitionWatermarkTracker::mark_active(
  self : PartitionWatermarkTracker,
  partition_id : Int,
) -> Unit {
  if partition_id >= 0 && partition_id < self.num_partitions {
    self.is_idle[partition_id] = false
  }
}

///|
/// Returns the current operator-level aligned watermark.
pub fn PartitionWatermarkTracker::get_operator_watermark(
  self : PartitionWatermarkTracker,
) -> @types.Watermark {
  self.operator_watermark
}

///|
/// Returns the current watermark for a specific channel.
pub fn PartitionWatermarkTracker::get_channel_watermark(
  self : PartitionWatermarkTracker,
  partition_id : Int,
) -> @types.Watermark? {
  if partition_id >= 0 && partition_id < self.num_partitions {
    Some(self.channel_watermarks[partition_id])
  } else {
    None
  }
}
