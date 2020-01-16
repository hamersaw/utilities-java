package com.bushpath.anamnesis.checksum;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

public class ChecksumFactory {
    public static Checksum buildDefaultChecksum() {
        return buildChecksum(HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C);
    }

    public static Checksum buildChecksum(HdfsProtos.ChecksumTypeProto type) {
        return new NativeChecksumCRC32();

        // TODO - handle this correctly
        /*switch (type) {
        case CHECKSUM_CRC32:
            return new ChecksumJavaCRC32();
        case CHECKSUM_CRC32C:
            return new ChecksumJavaCRC32C();
        default:
            System.out.println("CHECKSUM TYPE DOES NOT EXIST");
            return null;
        }*/
    }
}
