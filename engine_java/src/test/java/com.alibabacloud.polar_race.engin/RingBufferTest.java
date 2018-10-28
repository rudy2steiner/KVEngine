package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.common.io.ByteEvent;
import com.alibabacloud.polar_race.engine.common.io.ByteEventHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Null;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.junit.Test;
import org.omg.PortableServer.LIFESPAN_POLICY_ID;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class RingBufferTest {

    @Test
    public void test(){


// Executor that will be used to construct new threads for consumers

//
//        // Connect the handler
//        disruptor.handleEventsWith(new ByteEventHandler());
//
//        // Start the Disruptor, starts all threads running
//        disruptor.start();
//
//        // Get the ring buffer from the Disruptor to be used for publishing.
//
//           ringBuffer.publishEvent((event, sequence, buffer) -> event.set(buffer.getLong(0)), bb);

    int v=98;
    byte[] a=new byte[4];
        Bytes.int2bytes(v,a,0);
     System.out.println(Bytes.bytes2int(a,0));
    }

    @Test
    public void binaySearch(){
        List<String> list=new ArrayList();
        list.add("2");
        list.add("4");
        list.add("7");
        list.add("9");
        int mid;
        long value=10;
        long midValue=-1;
        if(!Null.isEmpty(list)){
            int low=0;
            int high=list.size()-1;
            while(low<high){
                 mid=low+(high-low)/2;
                 midValue=Long.valueOf(list.get(mid));
                 if(midValue<value){
                     if(Long.valueOf(list.get(mid+1))>value)
                         break;
                     low=mid+1;
                 }else if(midValue>value){
                     high=mid-1;
                 }else break;

            }
            if(low==high) midValue=Long.valueOf(list.get(high));
            System.out.println(midValue);
        }

    }
}
