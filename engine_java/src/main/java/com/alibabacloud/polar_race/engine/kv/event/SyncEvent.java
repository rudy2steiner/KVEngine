package com.alibabacloud.polar_race.engine.kv.event;

import java.util.concurrent.TimeoutException;

public class SyncEvent implements Event<Long> {
    private final static Long NOT_DONE_ID =-1l;
    private volatile Long maxDoneTxId= NOT_DONE_ID;
    private volatile Long txId;
    private long startTimestamp;
    private long finishTimestamp;
    public SyncEvent(Long txId){
        this.txId = txId;
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
        startTimestamp=System.currentTimeMillis();
       if(!isDone()){
            wait(timeout);
            // wake up or timeout
            if(!isDone()){
                throw new TimeoutException(String.format("%d timeout after %d",txId,System.currentTimeMillis()-startTimestamp));
            }
       }
       finishTimestamp=System.currentTimeMillis();
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
}
