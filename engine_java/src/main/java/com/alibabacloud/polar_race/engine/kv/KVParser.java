package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface KVParser {

    /**
     *
     * @param  to to buffer
     * @param  from  from buffer
     *
     **/
     void parse(AbstractVisitor visitor,ByteBuffer to, ByteBuffer from) throws IOException;

     /**
      *  to ensure log format complete
      **/
     IOHandler doRecover(AbstractVisitor visitor, ByteBuffer to, ByteBuffer from) throws IOException;

}
