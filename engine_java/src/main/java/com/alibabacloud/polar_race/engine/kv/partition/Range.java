package com.alibabacloud.polar_race.engine.kv.partition;

import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * inclusive low,exclusive high
 * 对key 做分区，并保存分区内的key
 *
 **/
public class Range {
    private final static Logger logger= LoggerFactory.getLogger(Range.class);
    private long low;
    private long high;
    private volatile long[] slot;
    private static final int DEFAULT_INIT_LENGTH=1024;
    private volatile int initSize;
    private AtomicReference<Status> status=new AtomicReference<>();
    private AtomicInteger index=new AtomicInteger(0);
    public Range(long low,long high){
        this(low,high,DEFAULT_INIT_LENGTH);
    }
    public Range(long low,long high,int initSize){
        this.low=low;
        this.high=high;
        this.slot=new long[initSize];
        this.initSize=initSize;
        this.status.set(Status.NORMAL);
    }

    public long getLow() {
        return low;
    }

    public void add(long value){
       int i= index.getAndIncrement();
       if(i>=initSize){
           ensureCapacity();
       }
       slot[i]=value;
    }

    public int getSize(){
       return index.get();
    }

    public long[] getSlot(){
        return slot;
    }

    /**
     * 扩容
     *
     **/
    public  synchronized void ensureCapacity(){
        if(this.status.compareAndSet(Status.NORMAL,Status.MOVING)){
            logger.info("enlarge for "+toString());
            int newLength=(int)(slot.length*1.2);
            long[] copy = new long[newLength];
            System.arraycopy(slot, 0, copy, 0,
                    Math.min(slot.length, newLength));
            slot=copy;
            // update new length
            initSize=newLength;
            this.status.compareAndSet(Status.MOVING,Status.NORMAL);
            //notifyAll();
        }else{
//            try {
//               // wait();
//            }catch (InterruptedException e){
//                e.printStackTrace();
//            }
        }
    }
    public void setLow(long low) {
        this.low = low;
    }

    public long getHigh() {
        return high;
    }

    public void setHigh(long high) {
        this.high = high;
    }



    /**
     * @return  0 表示contain or -1 表示小于，1 表示大于
     **/
    public int contain(long key){
         if(Bytes.compareUnsigned(key,low)<0) return -1;
         if(Bytes.compareUnsigned(key,high)>=0) return 1;
         return 0;
    }

    @Override
    public String toString() {
        return "["+low+","+high+"]";
    }

    enum Status{
        NORMAL,MOVING
    }
}
