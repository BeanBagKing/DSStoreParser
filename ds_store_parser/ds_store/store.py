# -*- coding: utf-8 -*-
import binascii
import struct
import re
import io
from io import BytesIO
import hashlib

from . import buddy


class IlocCodec(object):
    @staticmethod
    def decode(bytesData):
        x, y, z, a = struct.unpack_from(b'>IIII', bytesData[:16])
        h_str = binascii.hexlify(bytesData).decode()

        r_value_hor = x if x != 4294967295 else "Null"
        r_value_ver = y if y != 4294967295 else "Null"
        r_value_idx = z if z != 4294967295 else "Null"

        return f"Location: ({r_value_hor}, {r_value_ver}), Selected Index: {r_value_idx}, Unknown: {h_str[24:32]}"


class IcvoCodec(object):
    @staticmethod
    def decode(bytesData):
        h_str = binascii.hexlify(bytesData).decode()
        i_type = bytes.fromhex(h_str[:8]).decode(errors="ignore")
        p_size = str(int(h_str[8:12], 16))
        g_align = bytes.fromhex(h_str[12:20]).decode(errors="ignore")
        g_align_loc = bytes.fromhex(h_str[20:28]).decode(errors="ignore")
        unknown = str(h_str[28:])

        return f"Type: {i_type}, IconPixelSize: {p_size}, GridAlign: {g_align}, GridAlignTo: {g_align_loc}, Unknown: {unknown}"


class Fwi0Codec(object):
    @staticmethod
    def decode(bytesData):
        w, x, y, z = struct.unpack_from(b'>HHHH', bytesData[:16])
        h_str = binascii.hexlify(bytesData).decode()

        h_array = (
            f'top: {w}',
            f'left: {x}',
            f'bottom: {y}',
            f'right: {z}',
            f'view_type: {bytes.fromhex(h_str[16:24]).decode(errors="ignore")}',
            f'Unknown: {h_str[24:32]}'
        )

        return ", ".join(h_array)


class DilcCodec(object):
    @staticmethod
    def decode(bytesData):
        u, v, w, x, y, z, a, b = struct.unpack_from(b'>IIIIIIII', bytesData[:32])
        h_str = binascii.hexlify(bytesData).decode()

        h_pos = f"IconPosFromRight: {4294967295 - int(h_str[16:24], 16)}" if int(h_str[16:24], 16) > 65535 else f"IconPosFromLeft: {int(h_str[16:24], 16)}"
        v_pos = f"IconPosFromBottom: {4294967295 - int(h_str[24:32], 16)}" if int(h_str[24:32], 16) > 65535 else f"IconPosFromTop: {int(h_str[24:32], 16)}"

        h_array = (
            f"Unk1: {h_str[:8]}",
            f"GridQuadrant: {int(h_str[8:12], 16)}",
            f"Unk2: {h_str[12:16]}",
            h_pos,
            v_pos,
            f"GridIconPosFromLeft: {int(h_str[32:40], 16)}",
            f"GridIconPosFromTop: {int(h_str[40:48], 16)}",
            f"Unk3: {h_str[48:56]}",
            f"Unk4: {h_str[56:64]}"
        )

        return ", ".join(h_array)


class PlistCodec(object):
    @staticmethod
    def decode(bytesData):
        try:
            return biplist.readPlistFromBytes(bytesData)
        except Exception as exp:
            return f"{exp}: {binascii.hexlify(bytesData).decode()}"


class BookmarkCodec(object):
    @staticmethod
    def decode(bytesData):
        try:
            return mac_alias.Bookmark.from_bytes(bytesData)
        except Exception as exp:
            return f"{exp}: {binascii.hexlify(bytesData).decode()}"


codecs = {
    b'Iloc': IlocCodec,
    b'icvo': IcvoCodec,
    b'fwi0': Fwi0Codec,
    b'dilc': DilcCodec,
    b'bwsp': PlistCodec,
    b'lsvp': PlistCodec,
    b'glvp': PlistCodec,
    b'lsvP': PlistCodec,
    b'icvp': PlistCodec,
    b'lsvC': PlistCodec,
    b'pBBk': BookmarkCodec,
    b'pBB0': BookmarkCodec
}

codes = {
    "BKGD": u"Finder Folder Background Picture",
    "ICVO": u"Icon View Options",
    "Iloc": u"Icon Location",              # Location and Index
    "LSVO": u"List View Options",
    "bwsp": u"Browser Window Properties",
    "cmmt": u"Finder Comments",
    "clip": u"Text Clipping",
    "dilc": u"Desktop Icon Location",
    "dscl": u"Directory is Expanded in List View",
    "fdsc": u"Directory is Expanded in Limited Finder Window",
    "extn": u"File Extension",
    "fwi0": u"Finder Window Information",
    "fwsw": u"Finder Window Sidebar Width",
    "fwvh": u"Finder Window Sidebar Height",
    "glvp": u"Gallery View Properties",
    "GRP0": u"Group Items By",
    "icgo": u"icgo. Unknown. Icon View Options?",
    "icsp": u"icsp. Unknown. Icon View Properties?",
    "icvo": u"Icon View Options",
    "icvp": u"Icon View Properties",
    "icvt": u"Icon View Text Size",
    "info": u"info: Unknown. Finder Info?:",
    "logS": u"Logical Size",
    "lg1S": u"Logical Size",
    "lssp": u"List View Scroll Position",
    "lsvC": u"List View Columns",
    "lsvo": u"List View Options",
    "lsvt": u"List View Text Size",
    "lsvp": u"List View Properties",
    "lsvP": u"List View Properties",
    "modD": u"Modified Date",
    "moDD": u"Modified Date",
    "phyS": u"Physical Size",
    "ph1S": u"Physical Size",
    "pict": u"Background Image",
    "vSrn": u"Opened Folder in new tab",
    "bRsV": u"Browse in Selected View",
    "pBBk": u"Finder Folder Background Image Bookmark",
    "pBB0": u"Finder Folder Background Image Bookmark",
    "vstl": u"View Style Selected",
    "ptbL": u"Trash Put Back Location",
    "ptbN": u"Trash Put Back Name"
}

types = (
    'long',
    'shor',
    'blob',
    'dutc',
    'type',
    'bool',
    'ustr',
    'comp'
)
    

class DSStoreEntry(object):
    def __init__(self, filename, code, typecode, value=None, node=None):
        if isinstance(filename, bytes):
            filename = filename.decode('utf-8', errors="ignore")
        self.filename = filename
        self.code = code.decode() if isinstance(code, bytes) else code
        self.type = typecode
        self.value = value
        self.node = node

    def __repr__(self):
        return repr((self.filename, self.code, self.type, self.value, self.node))

    @classmethod
    def read(cls, block, node):
        nlen = struct.unpack('>I', block.read(4))[0]
        filename = block.read(2 * nlen).decode('utf-16be', errors="ignore")

        code, typecode = struct.unpack('>4s4s', block.read(8))

        if typecode == b'bool':
            value = struct.unpack('>?', block.read(1))[0]
        elif typecode in [b'long', b'shor']:
            value = struct.unpack('>I', block.read(4))[0]
        elif typecode == b'blob':
            vlen = struct.unpack('>I', block.read(4))[0]
            value = block.read(vlen)
            if code in codecs:
                value = codecs[code].decode(value)
                typecode = codecs[code]
        elif typecode == b'ustr':
            vlen = struct.unpack('>I', block.read(4))[0]
            value = block.read(2 * vlen).decode('utf-16be', errors="ignore")
        elif typecode == b'type':
            value = struct.unpack('>4s', block.read(4))[0].decode(errors="ignore")
        elif typecode in [b'comp', b'dutc']:
            value = struct.unpack('>Q', block.read(8))[0]
        else:
            raise ValueError(f'Unknown type code "{typecode}"')

        return DSStoreEntry(filename, code, typecode, value, node)

    def __lt__(self, other):
        return (self.filename.lower(), self.code) < (other.filename.lower(), other.code)

    def __le__(self, other):
        return (self.filename.lower(), self.code) <= (other.filename.lower(), other.code)

class DSStore:
    """Python 3 interface to a ``.DS_Store`` file."""
    
    def __init__(self, store):
        self._store = store
        self.entries = {}
        self.dict_list = {}
        self._superblk = self._store['DSDB']
        
        with self._get_block(self._superblk) as s:
            self._rootnode, self._levels, self._records, \
                self._nodes, self._page_size = s.read('>IIIII')
        
        self._min_usage = 2 * self._page_size // 3
        self._dirty = False
        self.src_name = self._store._file.name
    
    @classmethod
    def open(cls, file_or_name, mode='r+', initial_entries=None):
        store = buddy.Allocator.open(file_or_name, mode)
        return DSStore(store)
    
    def _get_block(self, number):
        return self._store.get_block(number)
    
    def _traverse(self, node):
        if node is None:
            node = self._rootnode
        
        with self._get_block(node) as block:
            next_node, count = block.read('>II')
            
            if next_node:
                for _ in range(count):
                    ptr = block.read('>I')[0]
                    yield from self._traverse(ptr)
                    
                    e = DSStoreEntry.read(block, node)
                    e_hash = self._generate_hash(e)
                    
                    if e_hash not in self.dict_list:
                        self.entries[e_hash] = e
                        self.entries[e_hash].node = f'allocated {node}'
                        self.dict_list[e_hash] = f'{e_hash} allocated {node}'
                    elif 'unallocated' in self.dict_list[e_hash]:
                        self.entries[e_hash] = e
                        self.entries[e_hash].node = f"{self.dict_list[e_hash].split('unallocated')[1]} reallocated in {node}"
                        self.dict_list[e_hash] += f', reallocated in {node}'
                
                yield from self._traverse(next_node)
                yield from self.entries.values()
                self.entries.clear()
            else:
                for _ in range(count):
                    e = DSStoreEntry.read(block, node)
                    e_hash = self._generate_hash(e)
                    
                    if e_hash not in self.dict_list:
                        self.entries[e_hash] = e
                        self.entries[e_hash].node = f'allocated {node}'
                        self.dict_list[e_hash] = f'{e_hash} allocated {node}'
                    elif 'unallocated' in self.dict_list[e_hash]:
                        self.entries[e_hash] = e
                        self.entries[e_hash].node = f"{self.dict_list[e_hash].split('unallocated')[1]} reallocated in {node}"
                        self.dict_list[e_hash] += f', reallocated in {node}'
                
                yield from self.entries.values()
                self.entries.clear()
    
    def __iter__(self):
        return self._traverse(self._rootnode)
    
    def _generate_hash(self, entry):
        hash_input = f"{entry.filename}{entry.type}{entry.code}{self.src_name}{entry.value}".encode('utf-8')
        return hashlib.md5(hash_input).hexdigest()
    
    def read_slack(self, slack, node):
        slack = bytes.fromhex(slack)
        search_exp = '(' + '|'.join(k + t for k in codes.keys() for t in types) + ')'
        p = re.compile(rb'\x00\x00\x00[\x01-\xff](\x00[\x01-\xff]){1,}' + search_exp.encode())
        s_offset = p.search(slack)
        if s_offset:
            s_offset = s_offset.span()[0]
        
        for match in re.finditer(rb'\x00\x00\x00[\x01-\xff](\x00[\x01-\xff]){1,}' + search_exp.encode(), slack):
            if match.start() == s_offset:
                prev = s_offset
                s_offset = None
            else:
                e_off = match.start()
                s_off = prev
                prev = e_off
                
                hex_str = slack[s_off:].hex()
                block = BytesIO(bytes.fromhex(hex_str))
                
                try:
                    nlen = struct.unpack('>I', block.read(4))[0]
                    filename = block.read(2 * nlen).decode('utf-16be')
                    code, typecode = struct.unpack('>4s4s', block.read(8))
                    
                    if typecode == b'bool':
                        value = struct.unpack('>?', block.read(4))[0]
                    elif typecode in [b'long', b'shor']:
                        value = struct.unpack('>I', block.read(4))[0]
                    elif typecode == b'blob':
                        vlen = struct.unpack('>I', block.read(4))[0]
                        value = block.read(vlen)
                        codec = codecs.get(code, None)
                        if codec:
                            value = codec.decode(value)
                            typecode = codec
                    elif typecode == b'ustr':
                        vlen = struct.unpack('>I', block.read(4))[0]
                        value = block.read(2 * vlen).decode('utf-16be')
                    elif typecode == b'type':
                        value = struct.unpack('>4s', block.read(4))[0]
                    elif typecode in [b'comp', b'dutc']:
                        value = struct.unpack('>Q', block.read(8))[0]
                    else:
                        raise ValueError(f'Unknown type code "{typecode}"')
                except Exception as e:
                    print(f'File: {self.src_name}. unable to parse entry. Error: {str(e)}')
                    continue
                
                e = DSStoreEntry(filename, code, typecode, value, 'unallocated')
                e_hash = self._generate_hash(e)
                
                if e_hash not in self.dict_list:
                    self.entries[e_hash] = e
                    self.dict_list[e_hash] = f'{e_hash} unallocated'
                elif 'unallocated' not in self.dict_list[e_hash]:
                    self.entries[e_hash] = e
                    self.entries[e_hash].node += f', reallocated in {node}'
                    self.dict_list[e_hash] = f'{e_hash} reallocated'
