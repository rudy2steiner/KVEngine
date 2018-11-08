package com.alibabacloud.polar_race.engine.example;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.ArraysComp;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import org.rocksdb.*;


public class RocksEngine extends AbstractEngine {
    RocksDB db;
    @Override
    public void open(String path) throws EngineException {
        Options options=new Options();
        options.setCreateIfMissing(true);
        try {
            db = RocksDB.open(options, path);
        }catch (RocksDBException e){
            throw new EngineException(RetCodeEnum.CORRUPTION,e.getMessage());
        }
    }

    @Override
    public void close(){
       FlushOptions options= new FlushOptions();
       options.setWaitForFlush(true);
       try {
           db.flush(options);
       }catch (RocksDBException e){
           e.printStackTrace();
       }finally {
           db.close();
       }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        try {
            db.put(key, value);
        }catch (RocksDBException e){
            throw new EngineException(RetCodeEnum.INCOMPLETE,e.getMessage());
        }
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        try {
            return db.get(key);
        }catch (RocksDBException e){
            throw new EngineException(RetCodeEnum.INCOMPLETE,e.getMessage());
        }
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
            RocksIterator it= db.newIterator();
            it.seek(lower);
            do{

                visitor.visit(it.key(),it.value());
                it.next();
                if(it.key()==null){
                    break;
                }

            }while (ArraysComp.compare(it.key(),upper)<0);
    }
}
