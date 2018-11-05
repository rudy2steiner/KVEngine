package com.alibabacloud.polar_race.engine.kv.wal;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.kv.Event;
import com.alibabacloud.polar_race.engine.kv.EventType;
import com.alibabacloud.polar_race.engine.kv.LogEvent;
import com.alibabacloud.polar_race.engine.kv.LogFileService;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import com.alibabacloud.polar_race.engine.kv.event.SyncEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.TimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MultiTypeEventHandler implements EventHandler<LogEvent<Event>>,TimeoutHandler {
    private final static Logger logger= LoggerFactory.getLogger(MultiTypeEventHandler.class);
    public final static ByteBuffer EMPTY_BUFFER=ByteBuffer.allocate(StoreConfig.EMPTY_FILL_BUFFER_SIZE);
    private final ByteBuffer valueIndexBuffer;
    private final static byte[]     longBytes=new byte[StoreConfig.LONG_LEN];
    private IOHandler handler;
    private LogFileService logFileService;
    private SyncEvent[] syncEvents;
    private int syncIndex=0;
    private long processedMaxTxId=-1;
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
        if(eventLogEvent.getValue().type()== EventType.SYNC){
            syncEvent=(SyncEvent) eventLogEvent.getValue();
            //  已经flush 完成
            if(syncEvent.value()<=flushedMaxTxId) {syncEvent.done(flushedMaxTxId);return;}
            syncEvents[syncIndex++]=syncEvent;
            if(syncIndex<StoreConfig.batchSyncSize) return;
            else flushAndAck(true);
        }else {
            put=(Put) eventLogEvent.getValue();
            tryRollLog(put);
            long offsetInFile=handler.length();
            // offset in file
            long offset=fileId+offsetInFile;
            put.value().setOffset(offset);
            //logger.info(String.format("handler %d %d",Bytes.bytes2long(put.value().getKey(),0),put.value().getOffset()));
            Bytes.short2bytes(put.value().size(),longBytes,0);
            handler.append(longBytes,0,StoreConfig.SHORT_LEN);
            handler.append(put.value().getKey());
            handler.append(put.value().getValue());
            // put value index
            valueIndexBuffer.put(put.value().getKey());
            valueIndexBuffer.putLong(offset);
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
            EMPTY_BUFFER.position((int)remain);
        }else {
            EMPTY_BUFFER.putChar('#');
        }
        EMPTY_BUFFER.flip();
        handler.append(EMPTY_BUFFER);
        flushValueIndex(true);
        //handler.flushBuffer();
        //flushAndAck(false);
        clearSyncEvent();
        String nextLogName=logFileService.nextLogName(handler);
        // roll to next log file
        handler=logFileService.bufferedIOHandler(nextLogName,handler);
        fileId=Long.valueOf(handler.name());
    }
    /**
     *
     */
    public boolean flushAndAck(boolean force) throws IOException{
        boolean flushed=false;
        if(flushedMaxTxId<processedMaxTxId) {
            handler.flush();
            //logger.info(String.format("%d flushed and ack,batch size %d", processedMaxTxId,syncIndex));
            flushedMaxTxId = processedMaxTxId;
            flushed=true;
        }
        clearSyncEvent();
        return flushed;
    }

    public void clearSyncEvent(){
        for (int i = 0; i < syncIndex; i++) {
            syncEvents[i].done(processedMaxTxId);
            syncEvents[i]=null;
        }
        syncIndex = 0;
    }


    /**
     *  flush tail and valueIndex buffer
     **/
    public void flushValueIndex(boolean roll) throws IOException{
        int size=valueIndexBuffer.position();
        // store tail and value index real size
        valueIndexBuffer.position(0);
        valueIndexBuffer.put(StoreConfig.verison);
        valueIndexBuffer.putInt(size);
        valueIndexBuffer.position(valueIndexBuffer.capacity());
        valueIndexBuffer.flip();
        if(roll)
            handler.append(valueIndexBuffer);
        else{
            handler.write(StoreConfig.SEGMENT_LOG_FILE_SIZE-valueIndexBuffer.capacity(),valueIndexBuffer);
        }
        handler.flush();
        valueIndexBuffer.clear();
    }

    /**
     * close
     **/
    public void flush0() throws IOException{
        flushValueIndex(false);
    }

    @Override
    public void onTimeout(long sequence) throws Exception {
        long start=System.currentTimeMillis();
        if(flushAndAck(false)){
            logger.info(Thread.currentThread().getId()+" on handler timeout and flush "+(System.currentTimeMillis()-start));
        }
    }
}