package com.alibabacloud.polar_race.engine.kv;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
public class LogEventProducerTranslator {

    private final RingBuffer<LogEvent<Cell>> ringBuffer;
    private EventTranslatorOneArg<LogEvent<Cell>,Cell> TRANSLATOR;
    public LogEventProducerTranslator(RingBuffer<LogEvent<Cell>> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
        this.TRANSLATOR=new LogEventTranslator();
    }

    public void publish(Cell cell)
    {
        ringBuffer.publishEvent(TRANSLATOR,cell);
    }

    public class LogEventTranslator implements EventTranslatorOneArg<LogEvent<Cell>,Cell> {

        @Override
        public void translateTo(LogEvent<Cell> cellLogEvent, long sequence, Cell cell) {
            cell.setTxId((int)sequence);
            cellLogEvent.setSequence(sequence);
            cellLogEvent.setValue(cell);
        }
    }
}
