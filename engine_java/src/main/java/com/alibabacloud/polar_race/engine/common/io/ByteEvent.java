package com.alibabacloud.polar_race.engine.common.io;

import com.lmax.disruptor.EventFactory;
import javafx.event.Event;

public final class ByteEvent {
    public final  static EventFactory<ByteEvent> EVENT_FACTORY=new EventFactory<ByteEvent>() {
        public ByteEvent newInstance() {
            return new ByteEvent();
        }
    };
    private long sequence;
    private volatile byte[] key;
    private volatile byte[] values;

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
