import pandas as pd

class BtreeV1:
    def __init__(self, file, offset):
        self._f = file
        self._o = offset

        self._size_of_offsets = 8

        self._f.seek(self._o)
        byts = self._f.read(8 + self._size_of_offsets * 2)
        assert byts[:4] == b"TREE"
        self._node_type = byts[4]  # 0 group, 1 dataset
        self._node_level = byts[5]  # 0 is root of the tree
        self._entries_used = int.from_bytes(byts[6:8], "little")
        self._entries_offset = file.tell()
        self._address_left_sibling = int.from_bytes(byts[8:8 + self._size_of_offsets], "little")
        self._address_right_sibling = int.from_bytes(
            byts[8 + self._size_of_offsets:8 + 2 * self._size_of_offsets], "little")

        n = self._entries_used + self._entries_used + 1
        bytsl = (
                self._entries_used * self._size_of_offsets +  # child entries
                self._entries_used * 24  # 1-4, 4-8, 1dim+64bit
        )
        byts = file.read(bytsl)

    @property
    def level(self):
        return self._node_level

    @property
    def type(self):
        return self._node_type

    @property
    def sibling_left(self):
        return self._address_left_sibling if self._address_left_sibling != self._f.undefined_address else None

    @property
    def sibling_right(self):
        return self._address_right_sibling if self._address_right_sibling != self._f.undefined_address else None


class BtreeV1Chunk(BtreeV1):
    def __init__(self, file, offset, dataset, keysize):
        super(BtreeV1Chunk, self).__init__(file, offset)
        self._dataset = dataset
        self._keysize = keysize

    def inspect_nodes(self):
        keysize = self._keysize
        for i in range(self._entries_used):
            offset = self._entries_offset + ((keysize + self._size_of_offsets) * i)
            self._f.seek(offset)
            kbyts = self._f.read(keysize)  # read the key
            byts = self._f.read(self._size_of_offsets)  # read the child pointer

            yield {
                "level": self._node_level,
                "entry": i,
                "offset": offset,
                "dataset": self._dataset,
            }

            if self.level != 0:
                child = BtreeV1Chunk(self._f, int.from_bytes(byts, "little"), self._dataset, keysize)
                yield from child.inspect_nodes()


fs = {
        "uas_Amon_IPSL-CM6A-LR_piControl_r1i1p1f1_gr_185001-234912.nc_cmip7repack4mb":
        {
            "time": [11861, 8+8*(1+1)],
            "lat": [5928, 8+8*(1+1)], # 8 + 8 * (ndim + 1)
            "time_bounds": [15715, 8+8*(2+1)],
            "lon": [8645, 8+8*(1+1)],
            "uas": [18663, 8+8*(3+1)],
        },
        "uas_Amon_IPSL-CM6A-LR_piControl_r1i1p1f1_gr_185001-234912.nc":
        {
            "lat": [2213, 8+8*(1+1)], # 8 + 8 * (ndim + 1)
            "lon": [4309, 8+8*(1+1)],
            "time": [31807, 8+8*(1+1)],
            "time_bounds": [33903, 8+8*(2+1)],
            "uas": [28671, 8+8*(3+1)],
        },
        "uas_day_IPSL-CM6A-LR_piControl_r1i1p1f1_gr_18500101-23491231.nc_cmip7repack4mb":
        {
            "time": [11861, 8+8*(1+1)],
            "lat": [5928, 8+8*(1+1)], # 8 + 8 * (ndim + 1)
            "time_bounds": [15715, 8+8*(2+1)],
            "lon": [8645, 8+8*(1+1)],
            "uas": [18663, 8+8*(3+1)],
        },
        "uas_day_IPSL-CM6A-LR_piControl_r1i1p1f1_gr_18500101-23491231.nc":
        {
            "lat": [2212, 8+8*(1+1)], # 8 + 8 * (ndim + 1)
            "lon": [4308, 8+8*(1+1)],
            "time": [31802, 8+8*(1+1)],
            "time_bounds": [33898, 8+8*(2+1)],
            "uas": [28666, 8+8*(3+1)],
        },
        "uas_3hr_IPSL-CM6A-LR_piControl_r1i1p1f1_gr_187001010300-197001010000.nc_cmip7repack4mb":
        {
            "time": [11861, 8+8*(1+1)],
            "lat": [5928, 8+8*(1+1)], # 8 + 8 * (ndim + 1)
            "time_bounds": [15715, 8+8*(2+1)],
            "lon": [8645, 8+8*(1+1)],
            "uas": [18663, 8+8*(3+1)],
        },
        "uas_3hr_IPSL-CM6A-LR_piControl_r1i1p1f1_gr_187001010300-197001010000.nc":
        {
            "lat": [2212, 8+8*(1+1)], # 8 + 8 * (ndim + 1)
            "lon": [4308, 8+8*(1+1)],
            "time": [31808, 8+8*(1+1)],
            "time_bounds": [33904, 8+8*(2+1)],
            "uas": [28672, 8+8*(3+1)],
        },
}

if __name__ == "__main__":
    for fname in fs:
        rows = []
        vs = fs[fname]
        with open(fname, "rb") as f:
            for v in vs:
                print(fname, v)
                btree = BtreeV1Chunk(f, vs[v][0], v, vs[v][1])
                rows.extend(list(btree.inspect_nodes()))
            df = pd.DataFrame(rows)
            df["file"] = fname

            if fname.endswith(".nc"):
                df.to_csv(fname.replace(".nc", ".csv"), index=False)
            elif fname.endswith(".nc_cmip7repack4mb"):
                df.to_csv(fname.replace(".nc_cmip7repack4mb", "_cmip7repack4mb.csv"), index=False)
