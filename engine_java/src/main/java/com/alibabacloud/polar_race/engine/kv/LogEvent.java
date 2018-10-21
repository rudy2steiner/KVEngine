package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.io.ByteEvent;
import com.lmax.disruptor.EventFactory;

public class LogEvent<T> {

    private T value;
    private  long sequence;

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }
}
