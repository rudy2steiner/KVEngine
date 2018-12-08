package com.alibabacloud.polar_race.engine.kv.partition;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * suspect negative and positive number count are equal
 *
 **/
public class LexigraphicalPartition implements Partition {
    private final static Logger logger= LoggerFactory.getLogger(LexigraphicalPartition.class);
    private long low,high;
    private Range[] partitions;
    private int partitionNum;
    private int partitionCapacity;
    private int maxPositiveIndex;
    /**
     * long 正数范围内的划分,如
     * [-10,10)
     * [-100,-1)
     * [0,100)
     **/
    public LexigraphicalPartition(long low,long high,int partitionNum){
        this(low,high,partitionNum,0);
    }
    public LexigraphicalPartition(long low,long high,int partitionNum,int partitionCapacity){
        this.low=low;
        this.high=high;
        this.partitionNum=partitionNum;
        this.partitions =new Range[partitionNum];
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
            index=negativePartition(low,-1,index);
            maxPositiveIndex=index-1;
            positivePartition(0,high,index);
            return ;
        }
        throw  new UnsupportedOperationException("unsupport ");
    }

    /**
     * postitive 范围内的partition
     * @return negative
     **/
    private int negativePartition(long low,long high,int indexStart){
        long negativeStep=low/(partitionNum/2)-10;  // step 是负数

        for(long i=high;i>low&&i<=0;i+=negativeStep){
            partitions[--indexStart]=new Range(Math.max(i+negativeStep,low),i,partitionCapacity);
        }
        //partitions[partitionNum-1].setHigh(-1);
        // Long.Max 作为负数划分的左界
        partitions[indexStart].setLow(Long.MAX_VALUE);
        return indexStart;
    }
    /**
     * postitive 范围内的partition
     **/
    private void positivePartition(long low,long high,int indexStart){
        long positiveStep=high/(partitionNum/2)+10; //little bigger
        long remain=high%(partitionNum/2);
        for(long i=high;i>remain&&i>=0;i-=positiveStep){
            partitions[--indexStart]=new Range(Math.max(0,i-positiveStep),i,partitionCapacity);
        }
        partitions[0].setLow(0);
    }

    /**
     * @return  partition id
     **/
    @Override
    public int partition(long key) {
        return binarySearch(key);
    }

    public Range getPartition(int partitionId){
        return partitions[partitionId];
    }

    @Override
    public int size() {
        return partitionNum;
    }

    /**
     * 一定在区间里
     * @return  partition id
     *
     **/
    private int binarySearch(long value){
         int low=0,high= partitions.length-1;
         int mid;
         int flag;
         while(low<=high){
              mid=(high-low)/2+low;
              flag= partitions[mid].contain(value);
              if(flag==0) return mid;
              if(flag<0) high=mid-1;
              if(flag>0) low=mid+1;
         }
         // bug or Long.max
        logger.info("bug,search "+value);
         // low==high
        // 最大数(-1)所在partition id
         return partitionNum-1;
    }

    public void sort(){
        for(int i=0;i<partitionNum;i++){
            partitions[i].sort();
        }
    }
    /**
     * @param lower
     * @param upper not inclusive
     *
     **/
    public void iterate(long lower,long upper ,RangeIterator iterator){
          int startPartitionId=binarySearch(lower);
          int endPartitionId=binarySearch(upper);
            Range range;
          if(startPartitionId==endPartitionId){
               range=getPartition(startPartitionId);
               range.iterate(lower,iterator);
          }else if(startPartitionId<endPartitionId){
              range=getPartition(startPartitionId);
              range.iterate(lower,iterator);
              for(int i=startPartitionId+1;i<endPartitionId;i++){
                  range=getPartition(i);
                  range.iterate(range.lowerKey(),iterator);
                }
              range=getPartition(endPartitionId);
              range.iterate(range.lowerKey(),upper,iterator);
          }else {
              logger.info("lexigraphical bug");
          }

    }




}
