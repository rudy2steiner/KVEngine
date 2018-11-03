package com.alibabacloud.polar_race.engine.kv.event;


import com.alibabacloud.polar_race.engine.kv.DoubleBuffer;
import com.alibabacloud.polar_race.engine.kv.Event;
import com.alibabacloud.polar_race.engine.kv.EventType;


public class IndexLogEvent implements Event<DoubleBuffer> {

    private volatile DoubleBuffer value;
    private long buckId;
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
}
