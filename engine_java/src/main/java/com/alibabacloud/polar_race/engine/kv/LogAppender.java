package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.Lifecycle;
import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.event.Cell;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LogAppender extends Service {
    public final  static EventFactory<LogEvent<Cell>> EVENT_FACTORY=new EventFactory<LogEvent<Cell>>() {
        public LogEvent<Cell> newInstance() {
            return new LogEvent<>();
        }
    };
    private final static ExecutorService executor = Executors.newSingleThreadExecutor();
    // Specify the expectedSize of the ring buffer, must be power of 2.
    private final static int DEFAULT_RING_BUFFER_SIZE = 1024*8;
    private int ringBufferSize;
    private LogEventProducerTranslator translator;
    // Construct the Disruptor
    private  static Disruptor<LogEvent<Cell>> disruptor;
    private  static RingBuffer<LogEvent<Cell>> ringBuffer;
    private LogEventHander eventHander;
    public LogAppender(IOHandler handler, LogFileService fileService , int ringBufferSize){
        this.ringBufferSize=ringBufferSize>0? Files.tableSizeFor(ringBufferSize):DEFAULT_RING_BUFFER_SIZE;
        this.disruptor = new Disruptor(EVENT_FACTORY, this.ringBufferSize, executor);
        this.ringBuffer = disruptor.getRingBuffer();
        this.eventHander=new LogEventHander(handler,fileService);
        this.disruptor.handleEventsWith(eventHander);
        this.translator =new LogEventProducerTranslator(ringBuffer);
    }

    public void append(Cell cell){
        translator.publish(cell);
    }


    public void onStop() throws Exception{
        disruptor.shutdown();
        executor.shutdown();
        eventHander.flush0();
    }

    public void onStart(){
        disruptor.start();
    }




}
