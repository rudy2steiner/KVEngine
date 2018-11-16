package com.alibabacloud.polar_race.engine.kv.event;

public class CacheEvent implements Event<Long> {

    private volatile EventType type;
    private volatile long  fileId;
    public CacheEvent(EventType type,long fileId){
        this.type=type;
        this.fileId=fileId;
    }
    @Override
    public EventType type() {
        return type;
    }

    @Override
    public Long value() {
        return fileId;
    }

    @Override
    public long txId() {
        return 0;
    }

    @Override
    public void setTxId(long txId) {

    }

    @Override
    public void set(Long aLong) {

    }
}
