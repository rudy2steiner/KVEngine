package com.alibabacloud.polar_race.engine.kv.wal;

import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.event.SyncEvent;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.lmax.disruptor.TimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class CommitLogService extends Service implements KVLogger, TimeoutHandler {
    private final static Logger logger= LoggerFactory.getLogger(CommitLogService.class);
    public final  ByteBuffer EMPTY_BUFFER=ByteBuffer.allocate(StoreConfig.EMPTY_FILL_BUFFER_SIZE);
    private final ByteBuffer valueIndexBuffer;
    private final  byte[]     shortByte=new byte[StoreConfig.SHORT_LEN];
    private IOHandler handler;
    private LogFileService logFileService;
    private SyncEvent[] syncEvents;
    private int syncIndex=0;
    private long processedMaxTxId=-1;
    private long flushedMaxTxId;
    private long fileId;
    private long timeoutAndNoEventCounter=0;
    private ThreadLocal<SyncEvent> syncs=new ThreadLocal<>();
    private ReentrantLock lock=new ReentrantLock();
    private volatile  long lastWriteTime;
    private ScheduledExecutorService scheduledExecutorService;
    private static final Random random=new Random(0);
    private int batchSyncSize=StoreConfig.MINI_batchSyncSize+random.nextInt(StoreConfig.batchSyncSize_FLUCTUATE);
    public CommitLogService(IOHandler handler, LogFileService logFileService,ScheduledExecutorService scheduledExecutorService){
        this.handler=handler;
        this.logFileService=logFileService;
        this.valueIndexBuffer=ByteBuffer.allocate(logFileService.tailerAndIndexSize());
        this.syncEvents=new SyncEvent[batchSyncSize];
        logger.info("batch size "+batchSyncSize);
        this.fileId=Long.valueOf(handler.name());
        // init value index buffer
        //this.valueIndexBuffer.put(StoreConfig.VERSION);
        this.valueIndexBuffer.position(StoreConfig.VALUE_INDEX_RECORD_SIZE);
        Bytes.short2bytes(StoreConfig.KEY_VALUE_SIZE,shortByte,0);
        this.scheduledExecutorService=scheduledExecutorService;
    }

    @Override
    public void onStart() throws Exception {
        //super.onStart();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if(System.currentTimeMillis()-lastWriteTime>5) {
                        if(lock.tryLock()) {
                            try {
                                onTimeout(1l);
                            } catch (Exception e) {
                                logger.info("timeout exception", e);
                            } finally {
                                lock.unlock();
                            }
                        }else {
                            //logger.info("hold lock too long,may bug");
                        }
                }
            }
        },10000,10, TimeUnit.MILLISECONDS);
        this.lastWriteTime=System.currentTimeMillis();
    }

    @Override
    public long put(byte[] key, byte[] value) throws Exception {
        lock.lock();
        lastWriteTime=System.currentTimeMillis();
        long remain= logFileService.logWritableSize()-handler.length();
        if(remain<StoreConfig.LOG_ELEMENT_SIZE ) {
            tryRollLog(remain);
        }
        long offsetInFile=handler.length();
        // offset in file
        long offset=fileId+offsetInFile;
        //put.value().setOffset(offset);
        //logger.info(String.format("handler %d %d",Bytes.bytes2long(put.value().getKey(),0),put.value().getOffset()));
        handler.append(shortByte);
        handler.append(key);
        handler.append(value);
        // put value index
        valueIndexBuffer.put(key);
        valueIndexBuffer.putLong(offset);
        //valueIndexBuffer.putInt(put.value().getTxId());
        long txId=++processedMaxTxId;
        //  已经flush 完成
        SyncEvent syncEvent=syncs.get();
        if(syncEvent==null){
            syncEvent=new SyncEvent();
            syncs.set(syncEvent);
        }
        syncEvent.set(txId);
        if(syncEvent.value()<=flushedMaxTxId) {syncEvent.done(flushedMaxTxId);lock.unlock();return txId;}
        syncEvents[syncIndex++]=syncEvent;
        if(syncIndex<batchSyncSize) {
            lock.unlock();
            try {
                // possible bug, flush before block
                syncEvent.get(StoreConfig.MAX_TIMEOUT);
            }catch (Exception e){
                logger.info("sync error",e);
            }
        }else {
            // syncEvent,no block thread
            flushAndAck(true);
            lock.unlock();
        }
        if(txId%100000==0){
            onAppendFinish(syncEvent,key);
        }
        return txId;
    }

    /**
     * on append finish
     **/
    public void onAppendFinish(SyncEvent syncEvent, byte[] key){
        logger.info(String.format("key %d,txId %d time elapsed %d ns", Bytes.bytes2long(key,0),
                syncEvent.txId(),syncEvent.elapse()));

    }


    @Override
    public void onStop() throws Exception {
        flush0();
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
     *
     * roll to next put file
     * @param remain
     **/
    public void tryRollLog(long remain) throws IOException {
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
        // roll to next put file
        handler=logFileService.bufferedIOHandler(nextLogName,handler);
        fileId=Long.valueOf(handler.name());
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
    @Override
    public void onTimeout(long l) throws Exception {
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
