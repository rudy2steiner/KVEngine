package com.alibabacloud.polar_race.engine.kv.index;




/***
 *
 *  index for a record
 *  sort 过程中去重逻辑：
 *  记录重复的key,及其最大的offset,
 *  和重复index 的Set,
 *  遍历，设置过期标志位
 * */
public class Index  {

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
