package com.alibabacloud.polar_race.collection;


import com.koloboke.compile.KolobokeMap;

@KolobokeMap
public abstract  class LongLongMap implements com.koloboke.collect.map.LongLongMap {

   public static LongLongMap withExpectedSize(int expectedSize){
       return new KolobokeLongLongMap(expectedSize);
   }

    @Override
    public long defaultValue() {
        return -1;
    }
}
