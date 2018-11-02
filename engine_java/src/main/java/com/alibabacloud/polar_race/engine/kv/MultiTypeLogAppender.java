package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.Lifecycle;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import com.alibabacloud.polar_race.engine.kv.event.SyncEvent;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiTypeLogAppender implements Lifecycle {


    public final  static EventFactory<LogEvent<Event>> EVENT_FACTORY=new EventFactory<LogEvent<Event>>() {
        public LogEvent<Event> newInstance() {
            return new LogEvent<>();
        }
    };
    private final static ExecutorService executor = Executors.newSingleThreadExecutor();
    // Specify the size of the ring buffer, must be power of 2.
    private final static int DEFAULT_RING_BUFFER_SIZE = 1024*8;
    private int ringBufferSize;
    private MultiTypeEventProducerTranslator translator;
    // Construct the Disruptor
    private  static Disruptor<LogEvent<Event>> disruptor;
    private  static RingBuffer<LogEvent<Event>> ringBuffer;
    private MultiTypeEventHandler eventHander;
    public MultiTypeLogAppender(IOHandler handler, LogFileService fileService , int ringBufferSize){
        this.ringBufferSize=ringBufferSize>0? Files.tableSizeFor(ringBufferSize):DEFAULT_RING_BUFFER_SIZE;
        this.disruptor = new Disruptor(EVENT_FACTORY, this.ringBufferSize, executor,ProducerType.MULTI,new TimeoutBlockingWaitStrategy(StoreConfig.MAX_TIMEOUT/5,TimeUnit.MILLISECONDS));
        this.ringBuffer = disruptor.getRingBuffer();
        this.eventHander=new MultiTypeEventHandler(handler,fileService);
        this.disruptor.handleEventsWith(eventHander);
        this.translator =new MultiTypeEventProducerTranslator(ringBuffer);
    }

    public long append(Put event) throws Exception{
        translator.publish(event);
        long txId=event.txId();
        SyncEvent syncEvent=new SyncEvent(txId);
        translator.publish(syncEvent);
        syncEvent.get(StoreConfig.MAX_TIMEOUT);
        return txId;
    }

    public void close() throws Exception{
        disruptor.shutdown();
        executor.shutdown();
        eventHander.flush0();
    }

    public void start(){
        disruptor.start();
    }
}
