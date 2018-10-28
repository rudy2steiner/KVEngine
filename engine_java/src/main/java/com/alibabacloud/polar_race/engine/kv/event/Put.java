package com.alibabacloud.polar_race.engine.kv.event;

import com.alibabacloud.polar_race.engine.kv.Cell;
import com.alibabacloud.polar_race.engine.kv.Event;
import com.alibabacloud.polar_race.engine.kv.EventType;

public class Put implements Event<Cell> {

    private Cell cell;
    public Put(Cell cell){
        this.cell=cell;
    }
    @Override
    public EventType type() {
        return EventType.PUT;
    }

    @Override
    public Cell value() {
        return cell;
    }

    @Override
    public long txId() {
        return cell.getTxId();
    }

    @Override
    public void setTxId(long txId) {
              cell.setTxId(txId);
    }
}
