package com.alibabacloud.polar_race.example;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.kv.Cell;
import com.alibabacloud.polar_race.engine.kv.WALog;
import com.alibabacloud.polar_race.engine.kv.WALogger;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogRingBufferEngine extends AbstractEngine {
    private final static Logger logger= LoggerFactory.getLogger(LogRingBufferEngine.class);
    private WALog walLogger;
    public LogRingBufferEngine(){

    }
    @Override
    public void open(String path) throws EngineException {
        try {
            this.walLogger = new WALogger(path);
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
            walLogger.log(new Put(new Cell(key, value)));
        }catch (Exception e){
            logger.error("write error",e);
            throw  new EngineException(RetCodeEnum.IO_ERROR,e.getMessage());
        }
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        try {
            return walLogger.get(key);
        }catch (Exception e){
            logger.error("read error",e);
            throw  new EngineException(RetCodeEnum.IO_ERROR,e.getMessage());
        }
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