package com.alibabacloud.polar_race.engine.kv;

public interface Event<T> {
     EventType type();
     T  value();
     long txId();
     void setTxId(long txId);

}
