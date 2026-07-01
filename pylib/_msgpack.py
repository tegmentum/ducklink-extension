"""A small, self-contained MessagePack implementation.

Covers the JSON-compatible value set plus byte strings: None, bool, int, float,
str, bytes, list, and dict. This keeps the reference worker dependency-free; in a
real worker you would use the `msgpack` package. Not a complete implementation
(no ext types, no streaming).
"""

from __future__ import annotations

import struct

# --- packing ---------------------------------------------------------------


def packb(obj) -> bytes:
    buf = bytearray()
    _pack(obj, buf)
    return bytes(buf)


def _pack(obj, buf: bytearray) -> None:
    if obj is None:
        buf.append(0xC0)
    elif obj is True:
        buf.append(0xC3)
    elif obj is False:
        buf.append(0xC2)
    elif isinstance(obj, int):  # note: bool handled above (bool is an int)
        _pack_int(obj, buf)
    elif isinstance(obj, float):
        buf.append(0xCB)
        buf += struct.pack(">d", obj)
    elif isinstance(obj, str):
        _pack_str(obj, buf)
    elif isinstance(obj, (bytes, bytearray)):
        _pack_bin(obj, buf)
    elif isinstance(obj, (list, tuple)):
        _pack_array(obj, buf)
    elif isinstance(obj, dict):
        _pack_map(obj, buf)
    else:
        raise TypeError(f"msgpack: cannot serialize {type(obj).__name__}")


def _pack_int(n: int, buf: bytearray) -> None:
    if 0 <= n <= 0x7F:
        buf.append(n)
    elif -32 <= n < 0:
        buf.append(n & 0xFF)  # negative fixint
    elif 0 <= n <= 0xFF:
        buf += bytes((0xCC, n))
    elif 0 <= n <= 0xFFFF:
        buf.append(0xCD)
        buf += struct.pack(">H", n)
    elif 0 <= n <= 0xFFFFFFFF:
        buf.append(0xCE)
        buf += struct.pack(">I", n)
    elif 0 <= n <= 0xFFFFFFFFFFFFFFFF:
        buf.append(0xCF)
        buf += struct.pack(">Q", n)
    elif -0x80 <= n < 0:
        buf.append(0xD0)
        buf += struct.pack(">b", n)
    elif -0x8000 <= n < 0:
        buf.append(0xD1)
        buf += struct.pack(">h", n)
    elif -0x80000000 <= n < 0:
        buf.append(0xD2)
        buf += struct.pack(">i", n)
    elif -0x8000000000000000 <= n < 0:
        buf.append(0xD3)
        buf += struct.pack(">q", n)
    else:
        raise OverflowError("msgpack: int out of 64-bit range")


def _pack_str(s: str, buf: bytearray) -> None:
    data = s.encode("utf-8")
    n = len(data)
    if n <= 31:
        buf.append(0xA0 | n)
    elif n <= 0xFF:
        buf += bytes((0xD9, n))
    elif n <= 0xFFFF:
        buf.append(0xDA)
        buf += struct.pack(">H", n)
    elif n <= 0xFFFFFFFF:
        buf.append(0xDB)
        buf += struct.pack(">I", n)
    else:
        raise OverflowError("msgpack: str too long")
    buf += data


def _pack_bin(data, buf: bytearray) -> None:
    n = len(data)
    if n <= 0xFF:
        buf += bytes((0xC4, n))
    elif n <= 0xFFFF:
        buf.append(0xC5)
        buf += struct.pack(">H", n)
    elif n <= 0xFFFFFFFF:
        buf.append(0xC6)
        buf += struct.pack(">I", n)
    else:
        raise OverflowError("msgpack: bytes too long")
    buf += bytes(data)


def _pack_array(seq, buf: bytearray) -> None:
    n = len(seq)
    if n <= 15:
        buf.append(0x90 | n)
    elif n <= 0xFFFF:
        buf.append(0xDC)
        buf += struct.pack(">H", n)
    elif n <= 0xFFFFFFFF:
        buf.append(0xDD)
        buf += struct.pack(">I", n)
    else:
        raise OverflowError("msgpack: array too long")
    for item in seq:
        _pack(item, buf)


def _pack_map(mapping: dict, buf: bytearray) -> None:
    n = len(mapping)
    if n <= 15:
        buf.append(0x80 | n)
    elif n <= 0xFFFF:
        buf.append(0xDE)
        buf += struct.pack(">H", n)
    elif n <= 0xFFFFFFFF:
        buf.append(0xDF)
        buf += struct.pack(">I", n)
    else:
        raise OverflowError("msgpack: map too long")
    for key, value in mapping.items():
        _pack(key, buf)
        _pack(value, buf)


# --- unpacking -------------------------------------------------------------


def unpackb(data: bytes):
    mv = memoryview(data)
    obj, pos = _unpack(mv, 0)
    if pos != len(mv):
        raise ValueError("msgpack: trailing bytes after value")
    return obj


def _unpack(mv: memoryview, pos: int):
    b = mv[pos]
    pos += 1
    if b <= 0x7F:  # positive fixint
        return b, pos
    if b >= 0xE0:  # negative fixint
        return b - 0x100, pos
    if 0x80 <= b <= 0x8F:  # fixmap
        return _unpack_map(mv, pos, b & 0x0F)
    if 0x90 <= b <= 0x9F:  # fixarray
        return _unpack_array(mv, pos, b & 0x0F)
    if 0xA0 <= b <= 0xBF:  # fixstr
        return _read_str(mv, pos, b & 0x1F)
    if b == 0xC0:
        return None, pos
    if b == 0xC2:
        return False, pos
    if b == 0xC3:
        return True, pos
    if b == 0xC4:
        n = mv[pos]
        return _read_bin(mv, pos + 1, n)
    if b == 0xC5:
        n = struct.unpack_from(">H", mv, pos)[0]
        return _read_bin(mv, pos + 2, n)
    if b == 0xC6:
        n = struct.unpack_from(">I", mv, pos)[0]
        return _read_bin(mv, pos + 4, n)
    if b == 0xCA:
        return struct.unpack_from(">f", mv, pos)[0], pos + 4
    if b == 0xCB:
        return struct.unpack_from(">d", mv, pos)[0], pos + 8
    if b == 0xCC:
        return mv[pos], pos + 1
    if b == 0xCD:
        return struct.unpack_from(">H", mv, pos)[0], pos + 2
    if b == 0xCE:
        return struct.unpack_from(">I", mv, pos)[0], pos + 4
    if b == 0xCF:
        return struct.unpack_from(">Q", mv, pos)[0], pos + 8
    if b == 0xD0:
        return struct.unpack_from(">b", mv, pos)[0], pos + 1
    if b == 0xD1:
        return struct.unpack_from(">h", mv, pos)[0], pos + 2
    if b == 0xD2:
        return struct.unpack_from(">i", mv, pos)[0], pos + 4
    if b == 0xD3:
        return struct.unpack_from(">q", mv, pos)[0], pos + 8
    if b == 0xD9:
        n = mv[pos]
        return _read_str(mv, pos + 1, n)
    if b == 0xDA:
        n = struct.unpack_from(">H", mv, pos)[0]
        return _read_str(mv, pos + 2, n)
    if b == 0xDB:
        n = struct.unpack_from(">I", mv, pos)[0]
        return _read_str(mv, pos + 4, n)
    if b == 0xDC:
        n = struct.unpack_from(">H", mv, pos)[0]
        return _unpack_array(mv, pos + 2, n)
    if b == 0xDD:
        n = struct.unpack_from(">I", mv, pos)[0]
        return _unpack_array(mv, pos + 4, n)
    if b == 0xDE:
        n = struct.unpack_from(">H", mv, pos)[0]
        return _unpack_map(mv, pos + 2, n)
    if b == 0xDF:
        n = struct.unpack_from(">I", mv, pos)[0]
        return _unpack_map(mv, pos + 4, n)
    raise ValueError(f"msgpack: unknown prefix 0x{b:02x}")


def _read_str(mv: memoryview, pos: int, n: int):
    return bytes(mv[pos : pos + n]).decode("utf-8"), pos + n


def _read_bin(mv: memoryview, pos: int, n: int):
    return bytes(mv[pos : pos + n]), pos + n


def _unpack_array(mv: memoryview, pos: int, n: int):
    out = []
    for _ in range(n):
        value, pos = _unpack(mv, pos)
        out.append(value)
    return out, pos


def _unpack_map(mv: memoryview, pos: int, n: int):
    out = {}
    for _ in range(n):
        key, pos = _unpack(mv, pos)
        value, pos = _unpack(mv, pos)
        out[key] = value
    return out, pos
