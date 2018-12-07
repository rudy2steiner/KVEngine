package com.alibabacloud.polar_race.engine.kv.wal;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.event.Event;
import com.alibabacloud.polar_race.engine.kv.event.EventType;
import com.alibabacloud.polar_race.engine.kv.LogEvent;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
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
    private final static byte[]     shortByte=new byte[StoreConfig.SHORT_LEN];
    private IOHandler handler;
    private LogFileService logFileService;
    private SyncEvent[] syncEvents;
    private int syncIndex=0;
    private long processedMaxTxId=-1;
    private long flushedMaxTxId;
    private Put put;
    private SyncEvent syncEvent;
    private long fileId;
    private long timeoutAndNoEventCounter=0;
    public MultiTypeEventHandler(IOHandler handler,LogFileService logFileService){
        this.handler=handler;
        this.logFileService=logFileService;
        this.valueIndexBuffer=ByteBuffer.allocate(logFileService.tailerAndIndexSize());
        this.syncEvents=new SyncEvent[StoreConfig.batchSyncSize];
        this.fileId=Long.valueOf(handler.name());
        // init value index buffer
        //this.valueIndexBuffer.put(StoreConfig.VERSION);
        this.valueIndexBuffer.position(StoreConfig.VALUE_INDEX_RECORD_SIZE);
    }
    @Override
    public void onEvent(LogEvent<Event> eventLogEvent, long sequence, boolean endOfBatch) throws Exception {
        if(eventLogEvent.getValue().type()== EventType.SYNC){
            onSyncEvent((SyncEvent) eventLogEvent.getValue());
        }else {
            onKVEvent((Put)eventLogEvent.getValue());
        }
        // need flush
    }


    /**
     * @param  event put event
     **/
    public void onKVEvent(Put event) throws Exception{
        put=event;
        tryRollLog(put.value().size());
        long offsetInFile=handler.length();
        // offset in file
        long offset=fileId+offsetInFile;
        put.value().setOffset(offset);
        //logger.info(String.format("handler %d %d",Bytes.bytes2long(put.value().getKey(),0),put.value().getOffset()));
        Bytes.short2bytes(put.value().size(),shortByte,0);
        handler.append(shortByte);
        handler.append(put.value().getKey());
        handler.append(put.value().getValue());
        // put value index
        valueIndexBuffer.put(put.value().getKey());
        valueIndexBuffer.putLong(offset);
        //valueIndexBuffer.putInt(put.value().getTxId());
        processedMaxTxId=put.value().getTxId();
    }

    public void onSyncEvent(SyncEvent sync) throws IOException{
        //  已经flush 完成
        if(sync.value()<=flushedMaxTxId) {sync.done(flushedMaxTxId);return;}
        syncEvents[syncIndex++]=sync;
        if(syncIndex<StoreConfig.batchSyncSize) return;
        else flushAndAck(true);
    }

    /**
     * @return  sync index
     * */
    public int getSyncIndex() {
        return syncIndex;
    }

    /**
     *
     * roll to next log file
     * @param logSize will write content byte size,not include length header of log protocol
     **/
    public void tryRollLog(int logSize) throws IOException {
        long remain= logFileService.logWritableSize()-handler.length();
        if(remain>=logSize+StoreConfig.LOG_KV_RECORD_LEAST_LEN) return;
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
        ackSyncEvent();
        String nextLogName=logFileService.nextLogName(handler);
        // roll to next log file
        handler=logFileService.bufferedIOHandler(nextLogName,handler);
        fileId=Long.valueOf(handler.name());
    }


    /**
     * 将缓存中的 索引 和 value刷盘，并ack request
     */
    public boolean flushAndAck(boolean force) throws IOException{
        boolean flushed=false;
        if(flushedMaxTxId<processedMaxTxId) {
            handler.flush();
            //logger.info(String.format("%d flushed and ack,batch expectedSize %d", processedMaxTxId,syncIndex));
            flushedMaxTxId = processedMaxTxId;
            flushed=true;
        }
        ackSyncEvent();
        return flushed;
    }


    /**
     * release sync flush request thread
     *
     **/
    public void ackSyncEvent(){
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
        // store tail and value index real expectedSize
        valueIndexBuffer.position(0);
        valueIndexBuffer.put(StoreConfig.VERSION);
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
        this.valueIndexBuffer.position(StoreConfig.VALUE_INDEX_RECORD_SIZE);
    }

    /**
     * asyncClose
     **/
    public void flush0() throws IOException{
        flushValueIndex(false);
        // 最后一个log 文件handler，退出时保正写入文件
        handler.closeFileChannel(false);
        LogBufferAllocator.release(handler.buffer());
    }

    /**
     *
     * handler timeout
     **/
    @Override
    public void onTimeout(long sequence) throws Exception {
        long start=System.currentTimeMillis();
        if(flushAndAck(false)){
            timeoutAndNoEventCounter=0;
            logger.info(Thread.currentThread().getId()+" on handler timeout and flush "+(System.currentTimeMillis()-start));
        }else{
            timeoutAndNoEventCounter++;
            if(timeoutAndNoEventCounter%100000==0)
                logger.info(Thread.currentThread().getId()+" timeout  and now write,consider asyncClose ");
        }
    }
}
