package com.alibabacloud.polar_race.engine.kv.index;



/***
 *
 *  index for a record
 *
 * */
public class Index {
    /***
     * file id:
     * range 0~250000
     **/
    private int fileId;
    /**
     *
     * offset in the file
     **/
    private long offsetInFile;

    public int getFileId() {
        return fileId;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    public long getOffsetInFile() {
        return offsetInFile;
    }

    public void setOffsetInFile(long offsetInFile) {
        this.offsetInFile = offsetInFile;
    }
}
