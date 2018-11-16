package com.alibabacloud.polar_race.engine.kv.event;


import com.alibabacloud.polar_race.engine.kv.buffer.DoubleBuffer;


public class IndexLogEvent implements Event<DoubleBuffer> {

    private volatile DoubleBuffer value;
    private volatile long buckId;
    public IndexLogEvent(DoubleBuffer value){
        this.value=value;
    }

    @Override
    public EventType type() {
        return EventType.INDEX;
    }

    @Override
    public DoubleBuffer value() {
        return value;
    }

    @Override
    public long txId() {
        return buckId;
    }

    @Override
    public void setTxId(long txId) {
        this.buckId=txId;
    }

    @Override
    public void set(DoubleBuffer doubleBuffer) {

    }
}
