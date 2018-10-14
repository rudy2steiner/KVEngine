package com.alibabacloud.polar_race;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.example.ExampleEngine;
import com.alibabacloud.polar_race.example.ExampleVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EngineStarter {
    private final static Logger logger= LoggerFactory.getLogger(EngineStarter.class);
    public static void main(String[] args){

        ExampleEngine engine=new ExampleEngine();
        try {
            engine.open("/export/rocksdb/");

            engine.write("1".getBytes(),"1".getBytes());
            engine.write("2".getBytes(),"2".getBytes());
            engine.write("3".getBytes(),"3".getBytes());
            engine.write("4".getBytes(),"4".getBytes());
            engine.write("5".getBytes(),"5".getBytes());
            engine.write("6".getBytes(),"6".getBytes());

            logger.info(new String(engine.read("1".getBytes())));
            engine.range("3".getBytes(),"5".getBytes(),new ExampleVisitor());
        }catch (EngineException e){

            logger.info("engine starter",e);
        }

    }


}
