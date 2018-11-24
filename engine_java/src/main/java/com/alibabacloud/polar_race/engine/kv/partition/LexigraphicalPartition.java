package com.alibabacloud.polar_race.engine.kv.partition;


/**
 *
 * suspect negative and positive number count are equal
 *
 **/
public class LexigraphicalPartition implements Partition {
    private long low,high;
    private Range[] slots;
    private int partitionNum;
    private int partitionCapacity;
    public LexigraphicalPartition(long low,long high,int partitionNum){
        this(low,high,partitionNum,0);
    }
    public LexigraphicalPartition(long low,long high,int partitionNum,int partitionCapacity){
        this.low=low;
        this.high=high;
        this.partitionNum=partitionNum;
        this.slots=new Range[partitionNum];
        this.partitionCapacity=partitionCapacity;
        partitionInit();
    }

    private void partitionInit(){
        long mid=-1;
        if(low<0&&high>=0){
            mid=0;
        }
        int index=partitionNum;
        if(mid==0){
            index=negativePartition(low,0,index);
            positivePartition(0,high,index);
            return ;
        }
        throw  new UnsupportedOperationException("unsupport ");
    }

    /**
     * postitive 范围内的partition
     **/
    private int negativePartition(long low,long high,int indexStart){
        long negativeStep=low/(partitionNum/2);
        for(long i=high;i>low&&i<=0;i+=negativeStep){
            slots[--indexStart]=new Range(Math.max(i+negativeStep,low),i,partitionCapacity);
        }
        slots[partitionNum-1].setHigh(-1);
        return indexStart;
    }
    /**
     * postitive 范围内的partition
     **/
    private void positivePartition(long low,long high,int indexStart){
        long positiveStep=high/(partitionNum/2);
        long remain=high%(partitionNum/2);
        for(long i=high;i>remain&&i>=0;i-=positiveStep){
            slots[--indexStart]=new Range(Math.max(0,i-positiveStep),i,partitionCapacity);
        }
        slots[0].setLow(0);
    }

    /**
     * @return  slot id
     **/
    @Override
    public int partition(long value) {
        return binarySearch(value);
    }

    public Range getPartition(int partitionId){
        return slots[partitionId];
    }

    /**
     * 一定在区间里
     * @return  slot id
     **/
    private int binarySearch(long value){
         int low=0,high=slots.length-1;
         int mid;
         int flag;
         while(low<high){
              mid=(high-low)/2+low;
              flag=slots[mid].contain(value);
              if(flag==0) return mid;
              if(flag<0) high=mid-1;
              if(flag>0) low=mid+1;
         }
         // low==high
         return low;
    }




}
