///|
/// Dynamic Bloom filter bitset for fast probabilistic set membership.
///
/// Prevents expensive disk/block searches for non-existent keys in SSTables.

///|
/// Bloom filter with double hashing over a 64-bit word bitset.
pub struct BloomFilter {
  words : FixedArray[Int64]
  num_bits : Int
  num_hashes : Int
}

///|
/// Computes a 64-bit FNV-1a hash of a string.
fn hash_fnv1a(str : String) -> (Int64, Int64) {
  let mut h1 : Int64 = -3750763034362895579L
  let mut h2 : Int64 = 5871781006564002453L

  for i = 0; i < str.length(); i = i + 1 {
    let code = str[i].to_int().to_int64()
    h1 = (h1 ^ code) * 1099511628211L
    h2 = (h2 ^ (code + 31L)) * 1442695040888963407L
  }

  (h1, h2)
}

///|
/// Creates a new BloomFilter sized for expected item count and target bits per key.
pub fn BloomFilter::new(
  expected_items : Int,
  bits_per_key : Int,
) -> BloomFilter {
  let mut total_bits = expected_items * bits_per_key
  if total_bits < 64 {
    total_bits = 64
  }
  let num_words = (total_bits + 63) / 64
  let words = FixedArray::make(num_words, 0L)
  let k = (bits_per_key * 69) / 100 // approximately ln(2) * bits_per_key
  let num_hashes = if k < 1 { 1 } else if k > 16 { 16 } else { k }

  { words, num_bits: num_words * 64, num_hashes }
}

///|
/// Adds a key string to the Bloom filter.
pub fn BloomFilter::add(self : BloomFilter, key : String) -> Unit {
  let (h1, h2) = hash_fnv1a(key)
  for i = 0; i < self.num_hashes; i = i + 1 {
    let combined = h1 + i.to_int64() * h2
    let mut bit_idx = (combined % self.num_bits.to_int64()).to_int()
    if bit_idx < 0 {
      bit_idx = bit_idx + self.num_bits
    }
    let word_idx = bit_idx / 64
    let bit_offset = bit_idx % 64
    let mask = 1L << bit_offset
    self.words[word_idx] = self.words[word_idx] | mask
  }
}

///|
/// Tests whether a key might be in the set.
/// If returns false, key is definitely not present. If true, key might be present.
pub fn BloomFilter::might_contain(self : BloomFilter, key : String) -> Bool {
  let (h1, h2) = hash_fnv1a(key)
  let mut present = true

  for i = 0; i < self.num_hashes; i = i + 1 {
    let combined = h1 + i.to_int64() * h2
    let mut bit_idx = (combined % self.num_bits.to_int64()).to_int()
    if bit_idx < 0 {
      bit_idx = bit_idx + self.num_bits
    }
    let word_idx = bit_idx / 64
    let bit_offset = bit_idx % 64
    let mask = 1L << bit_offset

    if (self.words[word_idx] & mask) == 0L {
      present = false
      break
    }
  }

  present
}

///|
/// Returns total bit capacity.
pub fn BloomFilter::get_num_bits(self : BloomFilter) -> Int {
  self.num_bits
}

///|
/// Returns number of hash iterations per key.
pub fn BloomFilter::get_num_hashes(self : BloomFilter) -> Int {
  self.num_hashes
}
