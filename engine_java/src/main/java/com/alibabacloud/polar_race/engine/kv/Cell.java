package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.StoreConfig;

import java.nio.ByteBuffer;

public class Cell {
    private final static int KEY_LENGTH=StoreConfig.KEY_SIZE;
    private byte[] key;
    private byte[] value;
    private long txId;
    private volatile long offset;

    public byte[] getKey() {
        return key;
    }

    public Cell(byte[] key,byte[] value){
        this.key=key;
        this.value=value;
    }

    public Cell(ByteBuffer buffer){
         if(buffer.remaining()>=KEY_LENGTH){
            key=new byte[KEY_LENGTH];
            buffer.get(key) ;
         }
         if(buffer.hasRemaining()){
            value=new byte[buffer.remaining()];
            buffer.get(value);
         }
    }
    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public long getTxId() {
        return txId;
    }

    public void setTxId(long txId) {
        this.txId = txId;
    }

    public int size(){
        return key.length+value.length;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
