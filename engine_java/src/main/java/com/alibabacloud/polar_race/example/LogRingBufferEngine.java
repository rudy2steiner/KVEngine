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
            this.walLogger.start();
        }catch (Exception e){
            throw  new EngineException(RetCodeEnum.CORRUPTION,e.getMessage());
        }

    }

    @Override
    public void close()  {
        try{
            this.walLogger.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        try {
            walLogger.log(new Cell(key, value));
        }catch (Exception e){
            throw  new EngineException(RetCodeEnum.IO_ERROR,e.getMessage());
        }
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
