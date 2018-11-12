package com.alibabacloud.polar_race.engine.common.utils;

public class MemoryInfo {

    private long total;
    private long used;
    private long free;
    private long shared;
    private long bufferCache;
    private long available;

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getUsed() {
        return used;
    }

    public void setUsed(long used) {
        this.used = used;
    }

    public long getFree() {
        return free;
    }

    public void setFree(long free) {
        this.free = free;
    }

    public long getShared() {
        return shared;
    }

    public void setShared(long shared) {
        this.shared = shared;
    }

    public long getBufferCache() {
        return bufferCache;
    }

    public void setBufferCache(long bufferCache) {
        this.bufferCache = bufferCache;
    }

    public long getAvailable() {
        return available;
    }

    public void setAvailable(long available) {
        this.available = available;
    }

    @Override
    public String toString() {
        return String.format("total %d,used %d,free %d, shared %d, buffer/cache %d, available %d",total,used,free,shared,bufferCache,available);
    }
}
