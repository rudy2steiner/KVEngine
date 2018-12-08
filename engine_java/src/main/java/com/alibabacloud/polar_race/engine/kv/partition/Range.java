package com.alibabacloud.polar_race.engine.kv.partition;


import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.KeyValueArray;
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
    private KeyValueArray partition;
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
        this.partition=new KeyValueArray(initSize);
        this.initSize=initSize;
        this.status.set(Status.NORMAL);
    }
    public Range(Index[] indexs,long low,long high){
        this.low=low;
        this.high=high;
        this.partition=new KeyValueArray(initSize);
        this.initSize=initSize;
        this.status.set(Status.NORMAL);
    }

    public long getLow() {
        return low;
    }
    /**
     * put index
     **/
    public void add(Index value){
       partition.put(value.getKey(),value.getOffset());
    }

    public void add(long key,int value){
        partition.put(key,value);
    }
    public int getSize(){
       return partition.getSize();
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
     * 左闭又开
     **/
    public int contain(long key){
         if(Bytes.compareUnsigned(key,low)<0) return -1;
         if(Bytes.compareUnsigned(key,high)>=0) return 1;
         return 0;
    }
    /**
     * <=
     *
     **/
    @Override
    public int ceiling(long key) {
        return binarySearch(key,true);
    }
    /**
     * >=
     *
     **/
    @Override
    public int floor(long key) {
        return binarySearch(key,false);
    }

    /**
     * @return  实际存储的最小key
     *
     **/
    public long lowerKey(){
        return partition.getKey(0);
    }

    /**
     * @return  s实际存储的最大key
     **/
    public long upperKey(){
        return partition.getKey(partition.getSize()-1);
    }
    /***
     * 找到了，直接返回
     * @param ceiling 范围检查标记
     *
     **/
    private int binarySearch(long key,boolean ceiling){
        //
        if(lowerKey()==key){
           return 0;
        }
        if(upperKey()==key){
           return partition.getSize()-1;
        }
        int low=0,high=partition.getSize()-1;
        int mid=-1;
        int compare=-1;
        while(low<=high){
            mid= (high-low)/2+low;
            //slotKey=slot[mid].getKey();
            compare=Bytes.compareUnsigned(partition.getKey(mid),key);
            if(compare==0) return mid; // found
            if(compare<0) low=mid+1;// a < b,向右搜索
            else high=mid-1;    // 向左搜索
        }
        // 防止超出范围key search
        if(compare<0&&ceiling){
            return Math.min(mid+1,partition.getSize()-1);
        }
        if(compare>0&&!ceiling){
            return Math.max(mid-1,0);
        }
        return mid;
    }

    /**
     * sort all the index in the partition
     **/
    public void sort(){
       partition.quickSort(partition.getKeys(),partition.getValues(),0,partition.getSize()-1);
    }

    @Override
    public void iterate(long lower, long upper, RangeIterator iterator) {
        Index index;
        int start=floor(lower);
        int end=ceiling(upper);
        if(upper==upperKey()){
            //exclude upper
            end--;
        }
        for(int i=start;i<=end;i++){
            // filter out of date key
            iterator.visit(partition.getKey(i),partition.getValue(i));
        }
    }

    @Override
    public void iterate(long lower, RangeIterator iterator) {
        Index index;
        int start=floor(lower);
        int end=getSize();
        for(int i=start;i<end;i++){
            //index=slot[i];
            // filter out of date key
            iterator.visit(partition.getKey(i),partition.getValue(i));
        }
    }



    @Override
    public String toString() {
        return "["+low+","+high+")";
    }

    public enum Status{
        NORMAL,MOVING
    }
}
