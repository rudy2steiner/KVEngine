package com.alibabacloud.polar_race.engine.kv.event;

public interface Event<T> {
     EventType type();
     T  value();
     void set(T t);
     long txId();
     void setTxId(long txId);
}
