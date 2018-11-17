package com.alibabacloud.polar_race.engine.common.io;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LogRingBuffer {

    private final static ExecutorService executor = Executors.newSingleThreadExecutor();
    // Specify the expectedSize of the ring buffer, must be power of 2.
    private final static int DEFAULT_BUFFER_SIZE = 10240;
    private int bufferSize=DEFAULT_BUFFER_SIZE;
   // private ByteEventProducerWithTranslator TRANSLATOR;
    // Construct the Disruptor
    private  static Disruptor<ByteEvent> disruptor;
    private  static RingBuffer<ByteEvent> ringBuffer;
    public LogRingBuffer(int bufferSize){
        this.bufferSize=bufferSize;
        this.disruptor = new Disruptor(new ByteEventFactory(),bufferSize,executor, ProducerType.MULTI,new BlockingWaitStrategy());
        this.ringBuffer = disruptor.getRingBuffer();
        disruptor.handleEventsWith(new ByteEventHandler());
        //this.TRANSLATOR=new ByteEventProducerWithTranslator(ringBuffer);
    }

    public void publish(byte[] key,byte[] value) throws Exception{

        long sequence = ringBuffer.next();  // Grab the next sequence
        try
        {
            ByteEvent event = ringBuffer.get(sequence); // Get the entry in the Disruptor
            // for the sequence
            System.arraycopy(key,0,event.getKey(),0,key.length);
            System.arraycopy(value,0,event.getValues(),0,value.length);
//            event.setKey(key);
//            event.setValues(value);
            event.setSequence(sequence);
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
    }

    public void close(){
        disruptor.shutdown();
        executor.shutdown();
    }

    public void start(){
        disruptor.start();
    }


}
