from __future__ import absolute_import

import io
import struct
from struct import error

from kafka.protocol.abstract import AbstractType


def _pack(f, value):
    try:
        return f(value)
    except error as e:
        raise ValueError("Error encountered when attempting to convert value: "
                        "{!r} to struct format: '{}', hit error: {}"
                        .format(value, f, e))


def _unpack(f, data):
    try:
        (value,) = f(data)
        return value
    except error as e:
        raise ValueError("Error encountered when attempting to convert value: "
                        "{!r} to struct format: '{}', hit error: {}"
                        .format(data, f, e))


class VarInt:
    @staticmethod
    def len_decoder(buffer):
        return VarInt.read_unsigned_varint(buffer) - 1

    @staticmethod
    def len_encoder(value):
        return VarInt.write_unsigned_varint(value+1).read()

    @staticmethod
    def read_varint(buffer):
        value = VarInt.read_unsigned_varint(buffer)
        return (value >> 1) ^ -(value & 1)

    @staticmethod
    def read_unsigned_varint(buffer):
        value = 0
        i = 0
        b = buffer.read(1)
        b = b[0] if len(b) > 0 else 0

        while (b & 0x80) != 0:
            value |= (b & 0x7f) << i
            i += 7
            if i > 28:
                raise Exception(value)
            b = buffer.read(1)
            b = b[0] if len(b) > 0 else 0
        value |= b << i
        return value

    @staticmethod
    def write_varint(value):
        return VarInt.write_unsigned_varint((value << 1) ^ (value >> 31))

    @staticmethod
    def write_unsigned_varint(value):
        buffer = io.BytesIO()
        while (value & 0xffffff80) != 0:
            b = (value & 0x7f) | 0x80
            buffer.write(struct.pack(">B", b))
            value = value >> 7
        buffer.write(struct.pack(">B", value))
        buffer.seek(0)
        return buffer


class Int8(AbstractType):
    _pack = struct.Struct('>b').pack
    _unpack = struct.Struct('>b').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(1))


class Int16(AbstractType):
    _pack = struct.Struct('>h').pack
    _unpack = struct.Struct('>h').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(2))


class Int32(AbstractType):
    _pack = struct.Struct('>i').pack
    _unpack = struct.Struct('>i').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(4))


class Int64(AbstractType):
    _pack = struct.Struct('>q').pack
    _unpack = struct.Struct('>q').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(8))


class String(AbstractType):
    len_encoder = Int16.encode
    len_decoder = Int16.decode

    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def encode(self, value):
        if value is None:
            return self.__class__.len_encoder(-1)
        value = str(value).encode(self.encoding)
        return self.__class__.len_encoder(len(value)) + value

    def decode(self, data):
        length = self.__class__.len_decoder(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding string')
        return value.decode(self.encoding)


class CompactString(String):
    len_encoder = VarInt.len_encoder
    len_decoder = VarInt.len_decoder


class Bytes(AbstractType):
    len_encoder = Int32.encode
    len_decoder = Int32.decode

    @classmethod
    def encode(cls, value):
        if value is None:
            return cls.len_encoder(-1)
        else:
            return cls.len_encoder(len(value)) + value

    @classmethod
    def decode(cls, data):
        length = cls.len_decoder(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding Bytes')
        return value

    @classmethod
    def repr(cls, value):
        return repr(value[:100] + b'...' if value is not None and len(value) > 100 else value)


class CompactBytes(Bytes):
    len_encoder = VarInt.len_encoder
    len_decoder = VarInt.len_decoder


class Boolean(AbstractType):
    _pack = struct.Struct('>?').pack
    _unpack = struct.Struct('>?').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(1))


class Schema(AbstractType):
    def __init__(self, *fields):
        if fields:
            self.names, self.fields = zip(*fields)
        else:
            self.names, self.fields = (), ()

    def encode(self, item):
        if len(item) != len(self.fields):
            raise ValueError('Item field count does not match Schema')
        return b''.join([
            field.encode(item[i])
            for i, field in enumerate(self.fields)
        ])

    def decode(self, data):
        return tuple([field.decode(data) for field in self.fields])

    def __len__(self):
        return len(self.fields)

    def repr(self, value):
        key_vals = []
        try:
            for i in range(len(self)):
                try:
                    field_val = getattr(value, self.names[i])
                except AttributeError:
                    field_val = value[i]
                key_vals.append('%s=%s' % (self.names[i], self.fields[i].repr(field_val)))
            return '(' + ', '.join(key_vals) + ')'
        except Exception:
            return repr(value)


class Array(AbstractType):
    len_encoder = Int32.encode
    len_decoder = Int32.decode

    def __init__(self, *array_of):
        if len(array_of) > 1:
            self.array_of = Schema(*array_of)
        elif len(array_of) == 1 and (isinstance(array_of[0], AbstractType) or
                                     issubclass(array_of[0], AbstractType)):
            self.array_of = array_of[0]
        else:
            raise ValueError('Array instantiated with no array_of type')

    def encode(self, items):
        if items is None:
            return self.__class__.len_encoder(-1)
        return b''.join(
            [self.__class__.len_encoder(len(items))] +
            [self.array_of.encode(item) for item in items]
        )

    def decode(self, data):
        length = self.__class__.len_decoder(data)
        if length == -1:
            return None
        return [self.array_of.decode(data) for _ in range(length)]

    def repr(self, list_of_items):
        if list_of_items is None:
            return 'NULL'
        return '[' + ', '.join([self.array_of.repr(item) for item in list_of_items]) + ']'


class CompactArray(Array):
    len_encoder = VarInt.len_encoder
    len_decoder = VarInt.len_decoder


class TaggedField(AbstractType):

    def decode(cls, data):
        tag = VarInt.read_unsigned_varint(data)
        size = VarInt.len_decoder(data)
        if size > 0:
            val = data.read(size-1)
            assert len(val) == size-1
        else:
            val = None
        return tag, val

    def encode(cls, value):
        tag, val = value
        size = -1 if val is None else len(val)
        return VarInt.write_unsigned_varint(tag).read() + VarInt.len_encoder(size) + val
