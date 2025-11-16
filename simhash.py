import hashlib
from typing import Dict

def hash24bit(string: str) -> str:
    """
    Given a string, return its 24-bit binary hash
    """
    # Hash the string with sha256d
    sha_hash = hashlib.sha256(string.encode()).digest()
    
    # Extract the first 24 bits of the hash
    first_24_bits = int.from_bytes(sha_hash[:3], byteorder='big')
    
    # Convert the 24-bit integer into a binary string
    binary_string = format(first_24_bits, '024b')
    
    return binary_string

def get_fingerprint(frequencies: Dict[str, int]) -> str:
    """
    Returns a binary fingerprint based off the given 24-bit binary string
    """
    hash_frequencies = dict()
    for word, frequency in frequencies.items():
        hash_frequencies[hash24bit(word)] = frequency
    
    bit_vector = [0 for _ in range(24)]
    for bit_hash, frequency in hash_frequencies.items():
        for i in range(len(bit_hash)):
            bit_vector[i] += int(bit_hash[i]) * frequency if bit_hash[i] == '1' else -1 * frequency

    fingerprint = ""
    for bit in bit_vector:
        fingerprint += "1" if int(bit) > 0 else "0"
    return fingerprint

def compare_fingerprints(fingerprint1: str, fingerprint2: str) -> float:
    """
    Compares two given binary strings, returning the percentage matched bits
    """
    numer = 0; denom = 0
    for i in range(len(fingerprint1)):
        if fingerprint1[i] == fingerprint2[i]: numer += 1
        denom += 1
    return numer / denom

def check_similarities(fingerprints: dict, fingerprint: str) -> str:
    """
    Given a set of fingerprints and a fingerprint,
    check if the given fingerprint is similar to any of the fingerprints in the set.
    Return url of matched fingerprint if similar

    Similarity is defined by 23/24 ratio or higher
    """

    for each_fingerprint in fingerprints:
        if compare_fingerprints(each_fingerprint, fingerprint) > 23/24: return fingerprints[each_fingerprint]
    return ""
