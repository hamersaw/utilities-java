package com.bushpath.hdfs_comm.checksum;

public abstract class Checksum {
    public abstract long compute(byte[] buffer, int offset, int length);
    public abstract void bulkCompute(byte[] buffer, int offset, int length,
            byte[] checksumBuffer, int checksumOffset, int checksumLength);
}
