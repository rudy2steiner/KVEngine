package com.alibabacloud.polar_race.engine.kv.wal;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.Lifecycle;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;


import java.io.IOException;

/**
 * write ahead logging
 *
 **/
public interface WALog<T> extends Lifecycle,KVLogger {
    @Deprecated
    long log(T cell) throws Exception;
    @Deprecated
    void iterate(AbstractVisitor visitor) throws IOException;
    void range(byte[] lower,byte[] upper,AbstractVisitor visitor) throws EngineException;
    byte[] get(byte[] key) throws Exception;
}
