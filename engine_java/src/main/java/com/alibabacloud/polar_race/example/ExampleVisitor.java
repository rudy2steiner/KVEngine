package com.alibabacloud.polar_race.example;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleVisitor extends AbstractVisitor {
    private final static Logger logger= LoggerFactory.getLogger(ExampleVisitor.class);
    @Override
    public void visit(byte[] key, byte[] value) {
        logger.info(String.format("k:%s ,v:%s",new String(key),new String(value)));
    }
}
