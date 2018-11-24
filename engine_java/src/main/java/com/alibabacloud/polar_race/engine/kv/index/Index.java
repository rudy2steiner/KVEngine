package com.alibabacloud.polar_race.engine.kv.index;



/***
 *
 *  index for a record
 *
 * */
public class Index {

    private long key;
    private int  offset;
    public Index(long key,int offset){
        this.key=key;
        this.offset=offset;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }
}
