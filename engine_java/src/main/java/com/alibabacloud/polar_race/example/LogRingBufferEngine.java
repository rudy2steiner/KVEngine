package com.alibabacloud.polar_race.example;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.kv.Cell;
import com.alibabacloud.polar_race.engine.kv.WALog;
import com.alibabacloud.polar_race.engine.kv.WALogImpl;

public class LogRingBufferEngine extends AbstractEngine {
    private WALog walLogger;
    public LogRingBufferEngine(){

    }
    @Override
    public void open(String path) throws EngineException {
        try {
            this.walLogger = new WALogImpl(path);
        }catch (Exception e){
            throw  new EngineException(RetCodeEnum.CORRUPTION,e.getMessage());
        }
        this.walLogger.start();
    }

    @Override
    public void close() {
        this.walLogger.close();
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        walLogger.log(new Cell(key,value));
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        return new byte[0];
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        try {
            walLogger.iterate(visitor);
        }catch (Exception e){
            throw  new EngineException(RetCodeEnum.CORRUPTION,e.getMessage());
        }
    }
}
