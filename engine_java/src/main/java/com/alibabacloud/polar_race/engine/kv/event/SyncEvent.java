package com.alibabacloud.polar_race.engine.kv.event;

import com.alibabacloud.polar_race.engine.common.recyclable;
import java.util.concurrent.TimeoutException;

public class SyncEvent implements Event<Long> , recyclable {
    private final static Long NOT_DONE_ID =-1l;
    private volatile Long maxDoneTxId= NOT_DONE_ID;
    private volatile Long txId;
    private long startTimestamp;
    private long finishTimestamp;
    public SyncEvent(Long txId){
        this.txId = txId;
    }
    public SyncEvent(){

    }
    @Override
    public EventType type() {
        return EventType.SYNC;
    }

    @Override
    public Long value() {
        return txId;
    }
    public synchronized void done(Long maxDoneTxId){
         if(maxDoneTxId>=this.txId) {
             this.maxDoneTxId=maxDoneTxId;
         }else{
             throw  new  IllegalArgumentException("done but less than the txID");
         }
         notify();
    }
    public boolean isDone(){
        if(!maxDoneTxId.equals(NOT_DONE_ID))
         return true;
        return false;
    }

    public synchronized Long get(long timeout) throws InterruptedException,TimeoutException{
        startTimestamp=System.nanoTime();
       if(!isDone()){
            wait(timeout);
            // wake up or timeout
            if(!isDone()){
                throw new TimeoutException(String.format("%d timeout after %d",txId,System.nanoTime()-startTimestamp));
            }
       }
       finishTimestamp=System.nanoTime();
       return txId;
    }


    @Override
    public long txId() {
        return txId;
    }

    @Override
    public void setTxId(long txId) {
       //this.txId=txId;
    }

    public long elapse(){
        return finishTimestamp-startTimestamp;
    }

    @Override
    public void set(Long aLong) {
           txId=aLong;
    }

    @Override
    public void free() {
        maxDoneTxId= NOT_DONE_ID;
        txId=NOT_DONE_ID;
        finishTimestamp=-1;
        startTimestamp=-1;
    }
}
