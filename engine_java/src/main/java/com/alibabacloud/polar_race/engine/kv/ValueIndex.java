package com.alibabacloud.polar_race.engine.kv;

public class ValueIndex {
    private byte[] key;
    private volatile long   offset;
    public ValueIndex(byte[] key,long offset){
        this.key=key;
        this.offset=offset;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
