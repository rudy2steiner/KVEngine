package com.alibabacloud.polar_race.engine.common.io;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;

public class ByteEventProducerWithTranslator {
    private final RingBuffer<ByteEvent> ringBuffer;
    private   EventTranslatorTwoArg<ByteEvent,byte[],byte[]> TRANSLATOR;
    public ByteEventProducerWithTranslator(RingBuffer<ByteEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
        this.TRANSLATOR=new ByteEventTranslator();
    }



    public void publish(byte[] key,byte[] values)
    {
        ringBuffer.publishEvent(TRANSLATOR,key,values);
    }

    public class ByteEventTranslator implements EventTranslatorTwoArg<ByteEvent,byte[],byte[]> {

        @Override
        public void translateTo(ByteEvent event, long sequence, byte[] key,byte[] values) {
            event.setKey(key);
            event.setValues(values);
            event.setSequence(sequence);
        }
    }
}
