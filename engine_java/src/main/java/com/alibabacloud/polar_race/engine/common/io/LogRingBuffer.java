package com.alibabacloud.polar_race.engine.common.io;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LogRingBuffer {

    private final static ExecutorService executor = Executors.newSingleThreadExecutor();
    // Specify the size of the ring buffer, must be power of 2.
    private final static int DEFAULT_BUFFER_SIZE = 10240;
    private int bufferSize=DEFAULT_BUFFER_SIZE;
    private ByteEventProducerWithTranslator TRANSLATOR;
    // Construct the Disruptor
    private  static Disruptor<ByteEvent> disruptor;
    private  static RingBuffer<ByteEvent> ringBuffer;
    public LogRingBuffer(int bufferSize){
        this.bufferSize=bufferSize;
        this.disruptor = new Disruptor(ByteEvent.EVENT_FACTORY, bufferSize, executor);
        this.ringBuffer = disruptor.getRingBuffer();
        disruptor.handleEventsWith(new ByteEventHandler());
        this.TRANSLATOR=new ByteEventProducerWithTranslator(ringBuffer);
    }

    public void publish(byte[] key,byte[] value){
        TRANSLATOR.publish(key,value);
    }

    public void close(){
        disruptor.shutdown();
        executor.shutdown();
    }

    public void start(){
        disruptor.start();
    }


}
