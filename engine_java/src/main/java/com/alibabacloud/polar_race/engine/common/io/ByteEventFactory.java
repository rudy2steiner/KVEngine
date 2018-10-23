package com.alibabacloud.polar_race.engine.common.io;

import com.lmax.disruptor.EventFactory;

public class ByteEventFactory implements EventFactory<ByteEvent>
{
    @Override
    public ByteEvent newInstance() {
        return new ByteEvent();
    }
}
