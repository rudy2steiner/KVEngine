package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LogEventHander implements EventHandler<LogEvent<Cell>> {
    private final static Logger logger= LoggerFactory.getLogger(LogEventHander.class);
    private final static ByteBuffer EMPTY_BUFFER=ByteBuffer.allocate(StoreConfig.empty_fill_buffer_size);
    private final static ByteBuffer eventBuf=ByteBuffer.allocate(StoreConfig.empty_fill_buffer_size);

    private IOHandler handler;
    private LogFileService logFileService;
    public LogEventHander(IOHandler handler, LogFileService logFileService){
        this.handler=handler;
        this.logFileService=logFileService;
    }
    @Override
    public void onEvent(LogEvent<Cell> cellLogEvent, long sequence, boolean batchEnd) throws Exception {
            tryRollLog(cellLogEvent);
            logger.info(String.format("%d,k:%s ",Thread.currentThread().getId(), Bytes.bytes2long(cellLogEvent.getValue().getKey(),0)));
            eventBuf.clear();
            eventBuf.putShort((short)cellLogEvent.getValue().size());
            eventBuf.put(cellLogEvent.getValue().getKey());
            eventBuf.put(cellLogEvent.getValue().getValue());
            eventBuf.flip();
            handler.append(eventBuf);
    }

    public void tryRollLog(LogEvent<Cell> cellLogEvent) throws IOException {
        long remain=StoreConfig.SEGMENT_FILE_SIZE-handler.length();
        if(remain>=cellLogEvent.getValue().size()+2) return;
        EMPTY_BUFFER.clear();
        if(remain>=2) {
            EMPTY_BUFFER.putShort((short) (0));
            EMPTY_BUFFER.limit((short) remain);
        }else {
            EMPTY_BUFFER.putChar('#');
        }
        EMPTY_BUFFER.flip();
        handler.append(EMPTY_BUFFER);
        handler.flushBuf();
        String nextLogName=logFileService.nextLogName(cellLogEvent.getValue());
        // roll to next log file
        IOHandler nextHandler=logFileService.getFileChannelIOHandler(nextLogName,StoreConfig.FILE_WRITE_BUFFER_SIZE);
        //nextHandler.setBuffer(handler.getBuffer());
        handler=nextHandler;
    }


}
