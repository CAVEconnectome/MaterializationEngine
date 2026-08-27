# Partition Function Specification

**Spec version:** `1.0.0`
**Status:** Stable

This document defines a portable metadata format for describing a deterministic
partition function, so that the same input is mapped to the same
partition across languages and platforms.

---

## 1. Versioning of this specification

The metadata carries a `spec_version` field. It is independent of any one
deployment's parameters — it versions *this format itself* — and follows
[Semantic Versioning](https://semver.org/) (`MAJOR.MINOR.PATCH`):

| Change | Meaning | Consumer obligation |
| --- | --- | --- |
| **MAJOR** | A field's meaning, structure, or the computation changes such that the same metadata could produce a *different* partition, or a field is renamed/removed. | Consumers **MUST** reject metadata whose `MAJOR` they do not support, rather than guess. |
| **MINOR** | Backward-compatible additions — new optional fields, new enum values that don't affect existing configs. | Consumers **SHOULD** ignore unknown fields and continue. |
| **PATCH** | Editorial clarifications, added test vectors, wording. No behavioral change. | No action required. |

The rule of thumb: if a change could ever move an input from one partition to
another for an otherwise-identical configuration, it is a **MAJOR** bump.

---

## 2. Metadata schema

```json
{
  "spec_version": "1.0.0",
  "scheme": "hash-modulo",
  "hash": {
    "algorithm": "MurmurHash3_x86_32",
    "seed": 0,
    "output": "uint32"
  },
  "key_encoding": {
    "type": "uint64",
    "bytes": 8,
    "byte_order": "little"
  },
  "partition_count": 1024,
  "partition_op": "hash_u32 mod partition_count"
}
```

## 3. Field reference

| Field | Type | Allowed values (v1) | Description |
| --- | --- | --- | --- |
| `spec_version` | string | SemVer | Version of this metadata format. See §1. |
| `scheme` | string | `"hash-modulo"` | The partitioning strategy. Only `hash-modulo` is defined in v1. |
| `hash.algorithm` | string | `"MurmurHash3_x86_32"` | Exact hash variant. The `x86` denotes 32-bit-architecture mixing; the `32` denotes a 32-bit output. **Not** the x64_128 variant. Currently this is the only hash supported but more could easily be added here. |
| `hash.seed` | integer | `0` .. `2^32 - 1` | Seed passed to the hash. |
| `hash.output` | string | `"uint64"`, `"uint32"`, `"uint128"`, … | How the hash result should be interpreted. |
| `key_encoding.type` | string | `"uint64"`, `"uint32"`, `"uint128"`, … | Logical type of the input key (unsigned integer). |
| `key_encoding.bytes` | integer | `>= 1` | Number of bytes the key is serialized to before hashing. Must match be compatible with type width. |
| `key_encoding.byte_order` | string | `"little"` \| `"big"` | Endianness of the serialized key. |
| `partition_count` | integer | `>= 1` | Number of output partitions. |
| `partition_op` | string | `"hash_u32 mod partition_count"` | The reduction from hash to partition index.|

## 4. Partition algorithm

Given an unsigned integer key `k` and the metadata above, the partition is
computed as follows. The order is significant.

1. **Serialize** `k` to exactly `key_encoding.bytes` bytes using
   `key_encoding.byte_order` endianness, producing a byte array `B`.
2. **Hash** `B` with the named algorithm and `seed`, and interpret the result
   as an unsigned integer `h` as specified by (`hash.output`).
3. **Reduce**: `partition = h mod partition_count`.
4. The result is an integer in the half-open range `[0, partition_count)`.

The reference for step 2 is Austin Appleby's public-domain MurmurHash3
(`MurmurHash3_x86_32`), as implemented in the Python library `mmh3`.

## 5. Reproduction notes

- **Variant.** `MurmurHash3_x86_32` is a distinct function from
  `MurmurHash3_x86_128` and `MurmurHash3_x64_128`. They produce different values
  even at the same output width because their internal mixing differs. Several
  libraries default to the x64_128 variant (e.g. Guava's `murmur3_128`); using
  one of those will silently produce a different partition.
- **Endianness.** The key must be hashed as raw bytes in the declared byte
  order — never as its decimal string form, and never in the host's native
  endianness if that differs from `byte_order`.
- **Unsigned reduction.** The modulo must be taken over the *unsigned* 32-bit
  value. Languages with sign-of-dividend modulo (C, Java, Go, Rust) will return
  a different (possibly negative) result if the hash is treated as signed first.
  Interpret as `uint32`, then reduce.
- **Power-of-two note.** When `partition_count` is a power of two, `2^32` is an
  exact multiple of it, so the mapping is perfectly uniform with no modulo bias,
  and `h mod partition_count` equals masking the low `log2(partition_count)` bits.
  This equivalence does **not** hold for non-power-of-two counts; always encode
  the operation as modulo.

## 6. Conformance test vectors

For `algorithm = MurmurHash3_x86_32`, `seed = 0`, `key_encoding = {uint64, 8,
little}`, `partition_count = 1024`. An implementation that reproduces the `hash_u32`
column is byte-for-byte conformant; the `partition` column is provided for
convenience.

| `key` (decimal) | `key` (hex) | `hash_u32` | `partition` |
| --- | --- | --- | --- |
| 0 | `0x0000000000000000` | 1669671676 | 764 |
| 1 | `0x0000000000000001` | 1392991556 | 324 |
| 2 | `0x0000000000000002` | 3323962100 | 756 |
| 255 | `0x00000000000000FF` | 4242213303 | 439 |
| 256 | `0x0000000000000100` | 2997559978 | 682 |
| 65535 | `0x000000000000FFFF` | 2037014853 | 325 |
| 1234567890 | `0x00000000499602D2` | 2080695519 | 223 |
| 81985529216486895 | `0x0123456789ABCDEF` | 4203775010 | 34 |
| 9223372036854775808 | `0x8000000000000000` | 1366273829 | 805 |
| 18446744073709551615 | `0xFFFFFFFFFFFFFFFF` | 1651860712 | 232 |

## 7. Reference implementation (Python)

```python
import mmh3  # mmh3.hash == MurmurHash3_x86_32

def partition_of(key: int, *, seed: int = 0, byte_order: str = "little",
                 key_bytes: int = 8, partition_count: int = 1024) -> int:
    if not (0 <= key < 2 ** (8 * key_bytes)):
        raise ValueError("key out of range")
    b = key.to_bytes(key_bytes, byte_order)
    h = mmh3.hash(b, seed, signed=False)   # interpret as uint32
    return h % partition_count
```