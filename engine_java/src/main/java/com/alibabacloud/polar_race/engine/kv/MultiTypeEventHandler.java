package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import com.alibabacloud.polar_race.engine.kv.event.SyncEvent;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MultiTypeEventHandler implements EventHandler<LogEvent<Event>> {
    private final static Logger logger= LoggerFactory.getLogger(MultiTypeEventHandler.class);
    private final static ByteBuffer EMPTY_BUFFER=ByteBuffer.allocate(StoreConfig.EMPTY_FILL_BUFFER_SIZE);
    private final ByteBuffer valueIndexBuffer;
    private final static byte[]     longBytes=new byte[StoreConfig.LONG_LEN];

    private IOHandler handler;
    private LogFileService logFileService;
    private SyncEvent[] syncEvents;
    private int syncIndex=0;
    private long processedMaxTxId;
    private long flushedMaxTxId;
    private Put put;
    private SyncEvent syncEvent;
    private long fileId;
    public MultiTypeEventHandler(IOHandler handler,LogFileService logFileService){
        this.handler=handler;
        this.logFileService=logFileService;
        this.valueIndexBuffer=ByteBuffer.allocate(logFileService.tailerAndIndexSize());
        this.syncEvents=new SyncEvent[StoreConfig.batchSyncSize];
        this.fileId=Long.valueOf(handler.name());
        // init value index buffer
        this.valueIndexBuffer.put(StoreConfig.verison);
        this.valueIndexBuffer.position(StoreConfig.VALUE_INDEX_RECORD_SIZE);
    }
    @Override
    public void onEvent(LogEvent<Event> eventLogEvent, long sequence, boolean endOfBatch) throws Exception {

        if(eventLogEvent.getValue().type()==EventType.SYNC){
            syncEvent=(SyncEvent) eventLogEvent.getValue();
            //  已经flush 完成
            if(syncEvent.value()<=flushedMaxTxId) {syncEvent.done(flushedMaxTxId);return;}
            syncEvents[syncIndex++]=syncEvent;
            if(syncIndex<StoreConfig.batchSyncSize) return;
            else flushAndAck();
        }else {
            put=(Put) eventLogEvent.getValue();
            tryRollLog(put);
            //put.value().size());
            long offsetInFile=handler.length();
            // offset in file
            put.value().setOffset(offsetInFile);
            Bytes.int2bytes(put.value().size(),longBytes,0);
            handler.append(longBytes,0,StoreConfig.INT_LEN);
            // put txId
//            Bytes.long2bytes(put.value().getTxId(),longBytes,0);
//            handler.append(longBytes,0,StoreConfig.LONG_LEN);
            handler.append(put.value().getKey());
            handler.append(put.value().getValue());
            // put value index

            valueIndexBuffer.put(put.value().getKey());
            valueIndexBuffer.putLong(offsetInFile);
            //valueIndexBuffer.putInt(put.value().getTxId());
            processedMaxTxId=put.value().getTxId();
        }

        // need flush


    }
    public void tryRollLog(Put put) throws IOException {
        long remain= logFileService.logWritableSize()-handler.length();
        if(remain>=put.value().size()+StoreConfig.LOG_KV_RECORD_LEAST_LEN) return;
        EMPTY_BUFFER.clear();
        if(remain>=StoreConfig.LOG_KV_RECORD_LEAST_LEN) {
            EMPTY_BUFFER.putShort((short) 0);
            // fill empty
            EMPTY_BUFFER.limit((short) remain);
        }else {
            EMPTY_BUFFER.putChar('#');
        }
        EMPTY_BUFFER.flip();
        handler.append(EMPTY_BUFFER);
        flushValueIndex(true);
        handler.flushBuffer();
        // handler.flush();
        String nextLogName=logFileService.nextLogName(handler);
        // roll to next log file
        handler=logFileService.bufferedIOHandler(nextLogName,handler);
    }

    /**
     *
     */
    public void flushAndAck() throws IOException{
         handler.flush();
         for(SyncEvent sync:syncEvents){
             sync.done(processedMaxTxId);
         }
         flushedMaxTxId=processedMaxTxId;
         logger.info(String.format("%d flushed and ack",processedMaxTxId));
    }


    /**
     *  flush tail and valueIndex buffer
     **/
    public void flushValueIndex(boolean roll) throws IOException{
        int size=valueIndexBuffer.position();
        valueIndexBuffer.position(1);
        valueIndexBuffer.putInt(size);
        valueIndexBuffer.position(valueIndexBuffer.capacity());
        valueIndexBuffer.flip();
        if(roll)
            handler.append(valueIndexBuffer);
        else{
            handler.write(StoreConfig.SEGMENT_LOG_FILE_SIZE-valueIndexBuffer.capacity(),valueIndexBuffer);
        }
    }

    /**
     * close
     **/
    public void flush0() throws IOException{
        flushValueIndex(false);
    }


}
