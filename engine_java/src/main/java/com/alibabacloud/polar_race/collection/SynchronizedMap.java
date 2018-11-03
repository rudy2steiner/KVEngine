package com.alibabacloud.polar_race.collection;


import com.koloboke.compile.KolobokeMap;
import com.koloboke.compile.MethodForm;
import com.koloboke.function.LongLongConsumer;



@KolobokeMap
public abstract class SynchronizedMap {

public static SynchronizedMap withExpectedSize(int expectedSize) {
        return new KolobokeSynchronizedMap(expectedSize);
        }

public final synchronized long get(long key) {
        return subGet(key);
        }


public final synchronized long put(long key, long value) {
        return subPut(key, value);
        }

public final synchronized int size() {
        return subSize();
        }
public final void forEach(LongLongConsumer action){
          forEachx(action);
}

@MethodForm("get")
abstract long subGet(long key);

@MethodForm("put")
abstract long subPut(long key, long value);

@MethodForm("size")
abstract int subSize();

@MethodForm("forEach")
public abstract void  forEachx(LongLongConsumer action);
}
