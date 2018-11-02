package com.alibabacloud.polar_race.engine.kv.event;

import com.alibabacloud.polar_race.engine.kv.Event;
import com.alibabacloud.polar_race.engine.kv.EventType;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class SyncEvent implements Event<Long> {
    private final static Long NOT_DONE_ID =-1l;
    private AtomicBoolean    done=new AtomicBoolean(false);
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
         if(maxDoneTxId>=this.maxDoneTxId) {
             this.maxDoneTxId=maxDoneTxId;
         }
         notify();
    }
    public boolean isDone(){
        if(maxDoneTxId!= NOT_DONE_ID)
         return true;
        return false;
    }

    public synchronized Long get(long timeout) throws InterruptedException,TimeoutException{
        startTimestamp=System.currentTimeMillis();
       if(!isDone()){
            wait(timeout);
            // wake up or timeout
            if(!isDone()){
                throw new TimeoutException(String.format("%d timeout after %d",txId,timeout));
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
       this.txId=txId;
    }

    public long elapse(){
        return finishTimestamp-startTimestamp;
    }
}
