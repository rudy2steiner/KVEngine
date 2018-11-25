package com.alibabacloud.polar_race.engine.kv.partition;

import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.kv.index.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * inclusive low,exclusive high
 * 对key 做分区，并保存分区内的key
 * 排序后，遍历一遍去重
 **/
public class Range implements NavigableArray{
    private final static Logger logger= LoggerFactory.getLogger(Range.class);
    private long low;
    private long high;
    private volatile Index[] slot;
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
        this.slot=new Index[initSize];
        this.initSize=initSize;
        this.status.set(Status.NORMAL);
    }
    public Range(Index[] indexs,long low,long high){
        this.low=low;
        this.high=high;
        this.slot=new Index[initSize];
        this.initSize=initSize;
        this.status.set(Status.NORMAL);
    }

    public long getLow() {
        return low;
    }

    public void add(Index value){
       int i= index.getAndIncrement();
       if(i>=initSize){
           ensureCapacity();
       }
       slot[i]=value;
    }

    public int getSize(){
       return index.get();
    }

    public Index[] getSlot(){
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
            Index[] copy = new Index[newLength];
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
    /**
     * >=
     *
     **/
    @Override
    public int ceiling(long key) {
        return binarySearch(key,true);
    }
    /**
     * <=
     *
     **/
    @Override
    public int floor(long key) {
        return binarySearch(key,false);
    }


    /***
     * 找到了，直接返回
     *
     **/
    private int binarySearch(long key,boolean ceiling){
        //
        if(slot[0].getKey()==key){
           return 0;
        }
        if(slot[index.get()-1].getKey()==key){
           return index.get()-1;
        }
        int low=0,high=index.get()-1;
        int mid=-1;
        int compare=-1;
        while(low<=high){
            mid= (high-low)/2+low;
            //slotKey=slot[mid].getKey();
            compare=Bytes.compareUnsigned(slot[mid].getKey(),key);
            if(compare==0) return mid; // found
            if(compare<0) low=mid+1;
            else high=mid-1;
        }
        // 防止超出范围key search
        if(compare<0&&ceiling){
            return Math.min(mid+1,index.get()-1);
        }
        if(compare>0&&!ceiling){
            return Math.max(mid-1,0);
        }
        return mid;
    }

    @Override
    public void iterate(int start, int end, RangeIterator iterator) {
        Index index;
        for(int i=start;i<end;i++){
            index=slot[i];
            iterator.visit(index.getKey(),index.getOffset());
        }
    }

    @Override
    public String toString() {
        return "["+low+","+high+"]";
    }

    enum Status{
        NORMAL,MOVING
    }
}
