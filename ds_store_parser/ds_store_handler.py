from .ds_store import store as ds_store
import datetime
import binascii
import struct
from time import gmtime, strftime

class DsStoreHandler:
    """Wrapper class for handling the DS Store artifact."""
    def __init__(self, file_io, location):
        self._file_io = file_io
        self.location = location
        self.ds_store = ds_store.DSStore.open(self._file_io, "rb")

    def __iter__(self):
        """Iterate the entries within the store.

        Yields:
            <DsStoreRecord>: The ds store entry record
        """
        for ds_store_entry in sorted(self.ds_store):
            yield DsStoreRecord(ds_store_entry)


class DsStoreRecord:
    """A wrapper class for the DSStoreEntry."""
    def __init__(self, ds_store_entry):
        self.ds_store_entry = ds_store_entry

    def as_dict(self):
        """Turn the internal DSStoreEntry into a dictionary.

        Returns:
            tuple: (dictionary representation of DSStoreEntry, node value)
        """
        record_dict = {
            "filename": self.ds_store_entry.filename,
            "type": self.ds_store_entry.type.__name__ if hasattr(self.ds_store_entry.type, "__name__") else self.ds_store_entry.type,
            "code": self.ds_store_entry.code,
            "value": self.ds_store_entry.value
        }

        # If type is "blob" and code is "modD" (Modified Date)
        if record_dict["type"] == "blob" and record_dict["code"].lower() == "modd":
            record_dict["value"] = binascii.hexlify(record_dict["value"]).decode("utf-8")

            a = record_dict["value"][:16]
            a = a[14:16] + a[12:14] + a[10:12] + a[8:10] + a[6:8] + a[4:6] + a[2:4] + a[:2]

            # Convert hex to float
            timestamp = struct.unpack(">d", bytes.fromhex(a))[0]
            parsed_dt = datetime.datetime.utcfromtimestamp(timestamp + 978307200)
            record_dict["value"] = parsed_dt

        elif record_dict["type"] == "blob":
            record_dict["value"] = binascii.hexlify(record_dict["value"]).decode("utf-8")

        elif record_dict["type"] == "dutc":
            epoch_dt = datetime.datetime(1904, 1, 1)
            parsed_dt = epoch_dt + datetime.timedelta(seconds=int(self.ds_store_entry.value) / 65536)
            record_dict["value"] = parsed_dt

        return record_dict, self.ds_store_entry.node
