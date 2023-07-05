import pyarrow as pa

class Uint256(pa.ExtensionType):

    def __init__(self):
        pa.ExtensionType.__init__(self, pa.binary(32), "Uint256")

    def __reduce__(self):
        return Uint256, ()
    
    @property
    def value(self):
        return self._value

    def __arrow_ext_serialize__(self):
        return "value={}".format(self.value).encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        serialized = serialized.decode()
        assert serialized.startswith("value=")
        value = serialized.split('=')[1]
        instance = Uint256().value(value)
        return instance

    def __eq__(self, other):
        if isinstance(other, pa.BaseExtensionType):
            return (type(self) == type(other) and
                    self.value == other.value)
        else:
            return NotImplemented

    # FIXME: implements 
    def __arrow_ext_class__(self):
        return NotImplemented

    # FIXME: implements 
    def to_pandas_dtype(self):
        return NotImplemented
 