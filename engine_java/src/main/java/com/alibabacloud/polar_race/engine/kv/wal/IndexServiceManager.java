package com.alibabacloud.polar_race.engine.kv.wal;

import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.kv.index.IndexService;

import java.util.ArrayList;
import java.util.List;


/***
 * 管理index 服务,处理启动顺序
 *
 **/
public class IndexServiceManager extends Service

    private List<IndexService> partitionIndexServices=new ArrayList<>();
    public IndexServiceManager(){


    }


    public void register(IndexService partitionIndexService){
        partitionIndexServices.add(partitionIndexService);
    }

    @Override
     public void onStart() throws Exception {
            //super.onStart();
    }


}
