package com.alibabacloud.polar_race;
import com.alibabacloud.polar_race.collection.LongSet;
import com.alibabacloud.polar_race.collection.LongLongMap;

import com.koloboke.function.LongLongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KolobokeMain {

    /**
     * Created by yeqinyong on 17/4/23.
     */
    private final static Logger logger= LoggerFactory.getLogger(KolobokeMain.class);
    public static void main(String[] args) throws InterruptedException {
            long start = System.currentTimeMillis();
            //SynchronizedMap rateMap = SynchronizedMap.withExpectedSize(1000_0000);
          LongLongMap rateMap = LongLongMap.withExpectedSize(1_00);
        System.out.println(rateMap.size());
            for (long i = 0; i < 400; i++) {
                rateMap.put(i, i);
            }
            rateMap.forEach(new LongLongConsumer() {
                int i=0;
                @Override
                public void accept(long key, long value) {
                    logger.info(String.format("key %d ,val %d,count %d",key,value,i++));
                }
            });
            long end = System.currentTimeMillis();
            System.out.println(end - start);
            System.out.println(rateMap.size());


        final LongSet intSet= LongSet.withExpectedSize(100);
        for(int i=0;i<1000;i++){
            intSet.add(i);
        }

        logger.info(intSet.removeLong(2)+"");


        Thread.sleep(1000_000);
    }

}
