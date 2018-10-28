package com.alibabacloud.polar_race.engine.kv.event;

import com.alibabacloud.polar_race.engine.kv.Event;
import com.alibabacloud.polar_race.engine.kv.EventType;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class SyncEvent implements Event<Long> {
    private final static Long NOT_DONE_ID =-1l;
    private AtomicBoolean    done=new AtomicBoolean(false);
    private volatile Long maxDoneTxId= NOT_DONE_ID;
    private volatile Long txId= NOT_DONE_ID;

    public SyncEvent(Long txId){
        this.txId = txId;
    }
    @Override
    public EventType type() {
        return null;
    }

    @Override
    public Long value() {
        return txId;
    }
    public  void done(Long maxDoneTxId){
         if(maxDoneTxId>=this.maxDoneTxId) {
             this.maxDoneTxId=maxDoneTxId;
         }
         notify();
    }
    public boolean isDone(){
        if(txId!= NOT_DONE_ID)
         return true;
        return false;
    }

    public Long get(long timeout) throws InterruptedException,TimeoutException{
       final long  maxTime=System.currentTimeMillis()+timeout;
       if(!isDone()){
            wait(timeout);
            // wake up or timeout
            if(!isDone()){
                throw new TimeoutException(String.format("%d timeout after %",txId,timeout));
            }
       }
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
}
