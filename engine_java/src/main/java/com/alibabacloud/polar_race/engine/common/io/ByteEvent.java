package com.alibabacloud.polar_race.engine.common.io;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.lmax.disruptor.EventFactory;
import javafx.event.Event;

public final class ByteEvent {
    private volatile long sequence;
    private volatile byte[] key;
    private volatile byte[] values;
    public ByteEvent(){
        this.key=new byte[StoreConfig.KEY_SIZE];
        this.values=new byte[StoreConfig.VALUE_SIZE];
    }


    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValues() {
        return values;
    }

    public void setValues(byte[] values) {
        this.values = values;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }
}
