package com.alibabacloud.polar_race.engine.kv.index;

import java.nio.ByteBuffer;

public interface IndexVisitor {

    void visit(ByteBuffer buffer) throws Exception;

    void onFinish() throws Exception;
}
