package com.alibabacloud.polar_race.engine.kv.wal;

import com.alibabacloud.polar_race.engine.kv.event.Event;
import com.alibabacloud.polar_race.engine.kv.LogEvent;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

public class MultiTypeEventProducerTranslator {

    private final RingBuffer<LogEvent<Event>> ringBuffer;
    private EventTranslatorOneArg<LogEvent<Event>,Event> TRANSLATOR;
    public MultiTypeEventProducerTranslator(RingBuffer<LogEvent<Event>> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
        this.TRANSLATOR=new MultiTypeEventProducerTranslator.EventTranslator();
    }

    public void publish(Event e)
    {
        ringBuffer.publishEvent(TRANSLATOR,e);
    }

    public class EventTranslator implements EventTranslatorOneArg<LogEvent<Event>,Event> {

        @Override
        public void translateTo(LogEvent<Event> logEvent, long sequence, Event e) {
            logEvent.setSequence(sequence);
            logEvent.setValue(e);
            e.setTxId(sequence);
        }
    }
}
