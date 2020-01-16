package com.bushpath.anamnesis.checksum;

public class NativeChecksumCRC32 extends Checksum {
    static {
        System.loadLibrary("crc");
    }

    @Override
    public long compute(byte[] buffer, int offset, int length) {
        return (long) nativeCompute(buffer, offset, length);
    }

    @Override
    public void bulkCompute(byte[] buffer, int offset, int length,
            byte[] checksumBuffer, int checksumOffset, int checksumLength) {
        nativeBulkCompute(buffer, offset, length,
            checksumBuffer, checksumOffset, checksumLength, 512);
    }

    public native int nativeCompute(byte[] buffer, int offset, int length);
    public native void nativeBulkCompute(byte[] buffer, int offset, int length, 
        byte[] checksumBuffer, int checksumOffset, int checksumLength, 
        int bytesPerChecksum);
}
