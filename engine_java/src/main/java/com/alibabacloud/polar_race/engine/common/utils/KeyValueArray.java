package com.alibabacloud.polar_race.engine.common.utils;
import com.alibabacloud.polar_race.engine.kv.partition.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicReference;

public class KeyValueArray {
    private final static Logger logger= LoggerFactory.getLogger(Range.class);
    private long keys[];
    private int  values[];
    private int size =0;
    private int capacity;
    private AtomicReference<Range.Status> status=new AtomicReference<>();
    public KeyValueArray(int capacity){
          keys=new long[capacity];
          values=new int[capacity];
          this.capacity=capacity;
    }


    /**
     *
     **/
    public synchronized void  put(long key,int value){
        if(size >=capacity){
            ensureCapacity();
        }
        keys[size]=key;
        values[size++]=value;
    }

    /**
     * @return  key
     **/
    public long getKey(int position){
        if(position>=size){
            throw new IndexOutOfBoundsException("out of array capacity");
        }
        return keys[position];
    }


    /**
     * @return  key
     **/
    public int getValue(int position){
        if(position>=size){
            throw new IndexOutOfBoundsException("out of array capacity");
        }
        return values[position];
    }
    /**
     * 1.2 倍扩容
     *
     **/
    public void ensureCapacity(){
        if(this.status.compareAndSet(Range.Status.NORMAL, Range.Status.MOVING)){
            logger.info("enlarge for "+toString());
            int newLength=(int)(capacity*1.2);
            long[] copyKey = new long[newLength];
            int[]  copyValue=new int[newLength];
            System.arraycopy(keys, 0, copyKey, 0,
                    Math.min(keys.length, newLength));
            System.arraycopy(values, 0, copyValue, 0,
                    Math.min(values.length, newLength));
            keys=copyKey;
            values=copyValue;
            // update new length
            capacity=newLength;
            this.status.compareAndSet(Range.Status.MOVING, Range.Status.NORMAL);
        }
    }

    public long[] getKeys() {
        return keys;
    }

    public void setKeys(long[] keys) {
        this.keys = keys;
    }

    public int[] getValues() {
        return values;
    }

    public void setValues(int[] values) {
        this.values = values;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    /**
     * @param a index to exchange
     * @param b
     * */
    public  void exch(long[] keys,int[] values,int a,int b){
        long tmp=keys[a];
        keys[a]=keys[b];
        keys[b]=tmp;
        // change the value ,as the same time
        tmp=values[a];
        values[a]=values[b];
        values[b]=(int)tmp;
    }

    /**
     * partition
     * @param lo  first index to sort
     * @param hi  largest index to sort ,inclusive
     **/
    private  int partition(long[] keys,int[] values,int lo,int hi){
        int i=lo,j=hi+1;
        long pivot=keys[lo];
        while(true){
            while(keys[++i]<pivot){
                if(i==hi) break;
            }
            while(pivot<keys[--j]){
                if(j==lo) break;
            }
            if(i>=j) break;
            exch(keys,values,i,j);
        }
        exch(keys,values,lo,j);
        return j;
    }

    /**
     *  key value sort,sort key with value
     *
     **/
    public  void quickSort(long[] keys,int[] values,int lo,int hi){
        if(hi<=lo) return;
        int j=partition(keys,values,lo,hi);
        quickSort(keys,values,lo,j-1);
        quickSort(keys,values,j+1,hi);
    }



}
