package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.Lifecycle;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * write ahead logging
 *
 **/
public interface WALog extends Lifecycle {
    long log(Cell cell);

    void iterate(AbstractVisitor visitor) throws IOException;

}
