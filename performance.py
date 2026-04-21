#!/usr/bin/env python
# coding: utf-8

import os
import time
import psutil

import numpy as np
import pandas as pd
import fsspec
import zarr
import pyfive


class Accessor:
    @property
    def name(self):
        return self._name

    @property
    def cache(self):
        return self._cache
    
    def open(self, store, object):
        raise NotImplementedError

    def locate(self, f, vname):
        v = f[vname]
        return v

    def load(self, v):
        return v[:].mean()

class Kerchunk(Accessor):
    def __init__(self, workers):
        self._name = "kerchunk"
        self._cache = None
        self._workers = workers

    def open(self, store, obj):
        url = store + "/" + obj + ".json"
        z = zarr.open(
                fsspec.filesystem(
                "reference",
                fo=url, 
                target_protocol="https",
                target_options={"asynchronous": False}, 
                remote_protocol="https",
                remote_options={"asynchronous": True},
                asynchronous=True,
            ).get_mapper(),
            mode="r")

        return z

class PyfiveCR(Accessor):
    def __init__(self, workers):
        self._name = "pyfive"
        self._cache = None
        self._workers = workers

    def open(self, store, obj):
        url = store + "/" + obj
        fs = fsspec.open(url, cache_type="first", block_size=2**20)
        byts = fs.open()
        self._cache = byts.cache
        f = pyfive.File(byts)

        return f

class PerfTestCR:
    def __init__(self):
        self._results = []
        self._store = "https://uor-aces-o.s3-ext.jc.rl.ac.uk"
        self._o = "bnl/uas_{}_IPSL-CM6A-LR_piControl_r1i1p1f1_gr_{}.{}_cmip7repack{}mb"
        self._o_orig = "bnl/uas_{}_IPSL-CM6A-LR_piControl_r1i1p1f1_gr_{}.{}"
        self._vname = "uas"

    @property
    def results(self):
        return self._results

    def measure(self, runs=1):
        for freq in ["Amon", "3hr", "day"]:
            for chunksize in ["4"]:
                for r in range(runs):
                    dates = {"Amon": "185001-234912", "day": "18500101-23491231", "3hr": "187001010300-197001010000"}
                    try:
                        w = 100
                        accessor = Kerchunk(w)
                        f = self._measure(r, accessor.name, "open", accessor, w, chunksize, freq, accessor.open,
                                          self._store, self._o.format(freq, dates[freq], "nc", chunksize))
                        v = self._measure(r, accessor.name, "locate", accessor, w, chunksize, freq, accessor.locate,f, self._vname)
                        result = self._measure(r, accessor.name, "load", accessor, w, chunksize, freq, accessor.load, v)
                
                        print(f"Accessor: {accessor.name}, result: {result:.2f}, run: {r}, workers: {w}.")
                
                        w = 101
                        accessor = Kerchunk(w)
                        f = self._measure(r, accessor.name, "open", accessor, w, chunksize, freq, accessor.open,
                                          self._store, self._o_orig.format(freq, dates[freq], "nc"))
                        v = self._measure(r, accessor.name, "locate", accessor, w, chunksize, freq, accessor.locate,f, self._vname)
                        result = self._measure(r, accessor.name, "load", accessor, w, chunksize, freq, accessor.load, v)
                
                        print(f"Accessor: {accessor.name}, result: {result:.2f}, run: {r}, workers: {w}.")

                        w = 99
                        accessor = PyfiveCR(w)
                        f = self._measure(r, accessor.name, "open", accessor, w, chunksize, freq, accessor.open,
                                         self._store, self._o.format(freq, dates[freq], "nc", chunksize))
                        v = self._measure(r, accessor.name, "locate", accessor, w, chunksize, freq, accessor.locate,f, self._vname)
                        result = self._measure(r, accessor.name, "load", accessor, w, chunksize, freq, accessor.load, v)
                
                        print(f"Accessor: {accessor.name}, result: {result:.2f}, run: {r}, workers: {w}.")
                    except:
                        raise
                        print(f"exception occured, {freq} {chunksize} {w} {r}")

    def _measure(self, run, pkg, name, accessor, workers, chunksize, freq, func, *args, **kwargs):
        start_net = psutil.net_io_counters()
        start_time = time.time()

        result = func(*args, **kwargs)
        
        end_time = time.time()
        end_net = psutil.net_io_counters()
        
        self._results.append({
            "run": run,
            "package": pkg,
            "name": name,
            "workers": workers,
            "chunksize": chunksize,
            "frequency": freq,
            "time": end_time - start_time,
            "bytes_recv": end_net.bytes_recv - start_net.bytes_recv,
            "bytes_sent": end_net.bytes_sent - start_net.bytes_sent,
            "packets_recv": end_net.packets_recv - start_net.packets_recv,
            "packets_sent": end_net.packets_sent - start_net.packets_sent,
            "errin": end_net.errin - start_net.errin,
            "errout": end_net.errout - start_net.errout,
            "dropin": end_net.dropin - start_net.dropin,
            "dropout": end_net.dropout - start_net.dropout,
            "cache_hits": accessor.cache.hit_count if accessor.cache else None,
            "cache_misses": accessor.cache.miss_count if accessor.cache else None,
        })

        return result

if __name__ == "__main__":
    t = PerfTestCR()
    t.measure(runs=2)
    
    df = pd.DataFrame.from_records(t.results)
    df.to_csv("results.csv", index=False)
