package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.Lifecycle;
import com.alibabacloud.polar_race.engine.common.io.ByteEvent;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LogAppender implements Lifecycle {
    public final  static EventFactory<LogEvent<Cell>> EVENT_FACTORY=new EventFactory<LogEvent<Cell>>() {
        public LogEvent<Cell> newInstance() {
            return new LogEvent<>();
        }
    };
    private final static ExecutorService executor = Executors.newSingleThreadExecutor();
    // Specify the size of the ring buffer, must be power of 2.
    private final static int DEFAULT_BUFFER_SIZE = 10240;
    private int bufferSize=DEFAULT_BUFFER_SIZE;
    private LogEventProducerTranslator translator;
    // Construct the Disruptor
    private  static Disruptor<LogEvent<Cell>> disruptor;
    private  static RingBuffer<LogEvent<Cell>> ringBuffer;
    public LogAppender(IOHandler handler,LogFileService fileService ,int bufferSize){
        this.bufferSize=bufferSize;
        this.disruptor = new Disruptor(EVENT_FACTORY, bufferSize, executor);
        this.ringBuffer = disruptor.getRingBuffer();
        this.disruptor.handleEventsWith(new LogEventHander(handler,fileService));
        this.translator =new LogEventProducerTranslator(ringBuffer);
    }

    public void append(Cell cell){
        translator.publish(cell);
    }

    public void close(){
        disruptor.shutdown();
        executor.shutdown();
    }

    public void start(){
        disruptor.start();
    }




}
