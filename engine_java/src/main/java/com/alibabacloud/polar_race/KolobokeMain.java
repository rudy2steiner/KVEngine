package com.alibabacloud.polar_race;
import com.alibabacloud.polar_race.collection.LongLongMap;
import com.alibabacloud.polar_race.collection.SynchronizedMap;
import com.koloboke.compile.KolobokeMap;
import com.koloboke.compile.MethodForm;
import com.koloboke.function.LongLongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
public class KolobokeMain {

    /**
     * Created by yeqinyong on 17/4/23.
     */
//
//    @KolobokeMap
//    abstract class MyMap<K, V> implements Map<K, V> {
//        static <K, V> Map<K, V> withExpectedSize(int expectedSize) {
//            return new KolobokeMyMap<K, V>(expectedSize);
//        }
//    }
//
//    @KolobokeMap
//    abstract class MyIntLongMap implements Map<Long, Short> {
//        static MyIntLongMap withExpectedSize(int expectedSize) {
//            return new KolobokeMyIntLongMap(expectedSize);
//        }
//
//        abstract short put(long key, short value);
//
//        abstract short get(long key);
//    }
//
//    @KolobokeMap
//    abstract class SynchronizedMap {
//        public static SynchronizedMap withExpectedSize(int expectedSize) {
//            return new KolobokeSynchronizedMap(expectedSize);
//        }
//
//        public final synchronized short get(long key) {
//            return subGet(key);
//        }
//
//        public final synchronized short put(long key, short value) {
//            return subPut(key, value);
//        }
//
//        public final synchronized int size() {
//            return subSize();
//        }
//
//        @MethodForm("get")
//        abstract short subGet(long key);
//
//        @MethodForm("put")
//        abstract short subPut(long key, short value);
//
//        @MethodForm("size")
//        abstract int subSize();
//    }
    private final static Logger logger= LoggerFactory.getLogger(KolobokeMain.class);
    public static void main(String[] args) throws InterruptedException {
            long start = System.currentTimeMillis();
            //SynchronizedMap rateMap = SynchronizedMap.withExpectedSize(1000_0000);
          LongLongMap rateMap = LongLongMap.withExpectedSize(1000_0000);
        System.out.println(rateMap.size());
            for (long i = 0; i < 4000_0; i++) {
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
            Thread.sleep(1000_000);
    }

}
