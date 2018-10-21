package com.alibabacloud.polar_race.engine.common.io;


import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteEventHandler implements EventHandler<ByteEvent> {
    int i=0;
    private final static Logger logger= LoggerFactory.getLogger(ByteEvent.class);
    public void onEvent(ByteEvent byteEvent, long sequence, boolean endOfBatch)  {

      //logger.info(String.format("sequence %d, %d,%s", sequence, Bytes.bytes2long(byteEvent.getKey(), 0), new String(byteEvent.getValues())));

        i++;
    }
}
