package com.alibabacloud.polar_race.engine.example;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.utils.Memory;
import com.alibabacloud.polar_race.engine.common.utils.Null;
import com.alibabacloud.polar_race.engine.kv.event.Cell;
import com.alibabacloud.polar_race.engine.kv.wal.WALog;
import com.alibabacloud.polar_race.engine.kv.wal.WALogger;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WALogEngine extends AbstractEngine {
    private final static Logger logger= LoggerFactory.getLogger(WALogEngine.class);
    private WALog walLogger;
    private ThreadLocal<Cell> cells=new ThreadLocal<>();
    private ThreadLocal<Put>  puts=new ThreadLocal<>();
    public WALogEngine(){

    }
    @Override
    public void open(String path) throws EngineException {
        logger.info("start before memory occupy "+Memory.memory().toString());
        try {
            if(!Null.isEmpty(path)) {
                String realPath=path;
                if(!path.endsWith("/")){
                    realPath+="/";
                }
                this.walLogger = new WALogger(realPath);
                this.walLogger.start();
            }else  throw new EngineException(RetCodeEnum.IO_ERROR,"path is empty");
        }catch (Exception e){
            logger.info("wal exception",e);
            throw  new EngineException(RetCodeEnum.CORRUPTION,e.getMessage());
        }
        logger.info("started memory occupy "+Memory.memory().toString());
    }

    @Override
    public void close()  {
        try{
            this.walLogger.close();
        }catch (Exception e){
            logger.info("close engine exception",e);
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        try {
            Cell cell=cells.get();
                if(cell==null){
                   cell=new Cell(null,null);
                   cells.set(cell);
                }
                cell.setKey(key);
                cell.setValue(value);
            Put put=puts.get();
                if(put==null){
                    put=new Put(null);
                    puts.set(put);
                }
            put.set(cell);
            walLogger.log(put);
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
            if(e instanceof EngineException) throw (EngineException)e;
            logger.error("read error",e);
            throw  new EngineException(RetCodeEnum.IO_ERROR,e.getMessage());
        }
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        try {
           // walLogger.iterate(visitor);
            throw new EngineException(RetCodeEnum.CORRUPTION,"not support");
        }catch (Exception e){
            throw  new EngineException(RetCodeEnum.CORRUPTION,e.getMessage());
        }
    }
}
