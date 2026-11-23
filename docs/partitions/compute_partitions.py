# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "mmh3>=5.2.1",
# ]
# ///

import mmh3


def partition_of(
    key: int,
    *,
    seed: int = 0,
    byte_order: str = "little",
    key_bytes: int = 8,
    partition_count: int = 1024,
    return_hash: bool = False,
) -> int:
    if not (0 <= key < 2 ** (8 * key_bytes)):
        raise ValueError("key out of range")
    b = key.to_bytes(key_bytes, byte_order)
    # mmh3.hash is MurmurHash3_x86_32
    h = mmh3.hash(b, seed, signed=False)  # interpret as uint32
    if return_hash:
        return h
    return h % partition_count


# Conformance test vectors
test_vectors = [
    (0, 1669671676, 764),
    (1, 1392991556, 324),
    (2, 3323962100, 756),
    (255, 4242213303, 439),
    (256, 2997559978, 682),
    (65535, 2037014853, 325),
    (1234567890, 2080695519, 223),
    (81985529216486895, 4203775010, 34),
    (9223372036854775808, 1366273829, 805),
    (18446744073709551615, 1651860712, 232),
]

print(f"{'key':<25} {'partition':<10} {'pass'}")
print("-" * 40)
for key, expected_hash, expected_partition in test_vectors:
    partition = partition_of(key)
    hash_u32 = partition_of(key, return_hash=True)
    ok = partition == expected_partition
    ok = ok and hash_u32 == expected_hash
    print(f"{key:<25} {partition:<10} {'✓' if ok else '✗'}")
