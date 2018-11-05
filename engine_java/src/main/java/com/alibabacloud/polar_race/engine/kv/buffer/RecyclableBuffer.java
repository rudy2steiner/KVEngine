package com.alibabacloud.polar_race.engine.kv.buffer;

public  interface RecyclableBuffer {
    /**
     * increase hold counter
     * @return  retained by  how many object
     **/
     int retain();
     /**
      * release underlying memory
      *
      **/
     void free();
     /**
      * @return true indicate free to use
      **/
     boolean available();
     /**
      * decrease hold counter
      * @return  retained by  how many object
      **/
     int release();
}
