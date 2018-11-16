package com.alibabacloud.polar_race.engine.kv.wal;
import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.event.Event;
import com.alibabacloud.polar_race.engine.kv.LogEvent;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import com.alibabacloud.polar_race.engine.kv.event.SyncEvent;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiTypeLogAppender extends Service {
    private final static Logger logger= LoggerFactory.getLogger(MultiTypeLogAppender.class);
    public final  static EventFactory<LogEvent<Event>> EVENT_FACTORY=new EventFactory<LogEvent<Event>>() {
        public LogEvent<Event> newInstance() {
            return new LogEvent<>();
        }
    };
    private final  ExecutorService executor = Executors.newSingleThreadExecutor();
    // Specify the size of the ring buffer, must be power of 2.
    private final static int DEFAULT_RING_BUFFER_SIZE = 1024;
    private int ringBufferSize;
    private MultiTypeEventProducerTranslator translator;
    // Construct the Disruptor
    private ThreadLocal<SyncEvent> syncs=new ThreadLocal<>();
    private  static Disruptor<LogEvent<Event>> disruptor;
    private  static RingBuffer<LogEvent<Event>> ringBuffer;
    private MultiTypeEventHandler eventHandler;
    public MultiTypeLogAppender(IOHandler handler, LogFileService fileService , int ringBufferSize){
        this.ringBufferSize=ringBufferSize>0? Files.tableSizeFor(ringBufferSize):DEFAULT_RING_BUFFER_SIZE;
        this.disruptor = new Disruptor(EVENT_FACTORY, this.ringBufferSize, executor,ProducerType.MULTI,new LiteTimeoutBlockingWaitStrategy(5,TimeUnit.MILLISECONDS));
        this.ringBuffer = disruptor.getRingBuffer();
        this.eventHandler =new MultiTypeEventHandler(handler,fileService);
        this.disruptor.handleEventsWith(eventHandler);
        this.translator =new MultiTypeEventProducerTranslator(ringBuffer);
    }

    /**
     * wal append
     *
     **/
    public long append(Put event) throws Exception{
        translator.publish(event);
        long txId=event.txId();
        SyncEvent syncEvent=syncs.get();
                  if(syncEvent==null){
                      syncEvent=new SyncEvent();
                      syncs.set(syncEvent);
                  }
                  syncEvent.set(txId);
        translator.publish(syncEvent);
                 syncEvent.get(StoreConfig.MAX_TIMEOUT);
        if(syncEvent.value()%1000000==0) {
            onAppendFinish(syncEvent, event);
        }
        // help gc
        event.value().free();
        syncEvent.free();
        return txId;
    }

    /**
     * on append finish
     **/
    public void onAppendFinish(SyncEvent syncEvent,Put event){
       logger.info(String.format("key %d,txId %d time elapsed %d", Bytes.bytes2long(event.value().getKey(),0),
                                  syncEvent.txId(),syncEvent.elapse()));

    }

    /**
     * shutdown gracefully
     *
     **/
    public void onStop() throws Exception{
        disruptor.shutdown();
        executor.shutdown();
        eventHandler.flush0();
    }

    /**
     *
     **/
    public void onStart(){
        disruptor.start();
    }
}
