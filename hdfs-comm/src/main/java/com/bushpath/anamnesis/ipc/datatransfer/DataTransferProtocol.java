package com.bushpath.anamnesis.ipc.datatransfer;

import com.google.protobuf.Message;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DataTransferProtocol {
    public static final int CHUNKS_PER_PACKET = 126, CHUNK_SIZE = 512;
    public static final int PROTOCOL_VERSION = 28;

    public static void sendBlockOpResponse(DataOutputStream out,
            DataTransferProtos.Status status) throws IOException {

        DataTransferProtos.BlockOpResponseProto blockOpResponseProto =
            DataTransferProtos.BlockOpResponseProto.newBuilder()
                .setStatus(status)
                .build();

        blockOpResponseProto.writeDelimitedTo(out);
        out.flush();
    }

    public static void sendBlockOpResponse(DataOutputStream out,
            DataTransferProtos.Status status, HdfsProtos.ChecksumTypeProto checksumType,
            int bytesPerChecksum, long chunkOffset) throws IOException {

        DataTransferProtos.ChecksumProto checksumProto =
            DataTransferProtos.ChecksumProto.newBuilder()
                .setType(checksumType)
                .setBytesPerChecksum(bytesPerChecksum)
                .build();

        DataTransferProtos.ReadOpChecksumInfoProto readOpChecksumInfoProto =
            DataTransferProtos.ReadOpChecksumInfoProto.newBuilder()
                .setChecksum(checksumProto)
                .setChunkOffset(chunkOffset)
                .build();

        DataTransferProtos.BlockOpResponseProto blockOpResponseProto =
            DataTransferProtos.BlockOpResponseProto.newBuilder()
                .setStatus(status)
                .setReadOpChecksumInfo(readOpChecksumInfoProto)
                .build();

        blockOpResponseProto.writeDelimitedTo(out);
        out.flush();
    }

    public static void sendReadOp(DataOutputStream out, String poolId, long blockId,
            long generationStamp, String client, long offset, long len)
            throws IOException {
        HdfsProtos.ExtendedBlockProto extendedBlockProto =
            HdfsProtos.ExtendedBlockProto.newBuilder()
                .setPoolId(poolId)
                .setBlockId(blockId)
                .setGenerationStamp(generationStamp)
                .build();

        DataTransferProtos.BaseHeaderProto baseHeaderProto =
            DataTransferProtos.BaseHeaderProto.newBuilder()
                .setBlock(extendedBlockProto)
                .build();

        DataTransferProtos.ClientOperationHeaderProto clientOperationHeaderProto =
            DataTransferProtos.ClientOperationHeaderProto.newBuilder()
                .setBaseHeader(baseHeaderProto)
                .setClientName(client)
                .build();

        Message proto = DataTransferProtos.OpReadBlockProto.newBuilder()
            .setHeader(clientOperationHeaderProto)
            .setOffset(offset)
            .setLen(len)
            .build();

        send(out, Op.READ_BLOCK, proto);
    }

    public static void sendWriteOp(DataOutputStream out, 
            DataTransferProtos.OpWriteBlockProto.BlockConstructionStage stage, 
            String poolId, long blockId, long generationStamp, String client,
            int pipelineSize) throws IOException {
        HdfsProtos.ExtendedBlockProto extendedBlockProto =
            HdfsProtos.ExtendedBlockProto.newBuilder()
                .setPoolId(poolId)
                .setBlockId(blockId)
                .setGenerationStamp(generationStamp)
                .build();

        DataTransferProtos.BaseHeaderProto baseHeaderProto =
            DataTransferProtos.BaseHeaderProto.newBuilder()
                .setBlock(extendedBlockProto)
                .build();

        DataTransferProtos.ClientOperationHeaderProto clientOperationHeaderProto =
            DataTransferProtos.ClientOperationHeaderProto.newBuilder()
                .setBaseHeader(baseHeaderProto)
                .setClientName(client)
                .build();

        DataTransferProtos.ChecksumProto checksumProto =
            DataTransferProtos.ChecksumProto.newBuilder()
                .setType(HdfsProtos.ChecksumTypeProto.CHECKSUM_NULL)
                .setBytesPerChecksum(-1)
                .build();

        Message proto = DataTransferProtos.OpWriteBlockProto.newBuilder()
            .setHeader(clientOperationHeaderProto)
            .setStage(stage)
            .setRequestedChecksum(checksumProto)
            .setPipelineSize(pipelineSize) // TODO - all these variables
            .setMinBytesRcvd(-1l)
            .setMaxBytesRcvd(-1l)
            .setLatestGenerationStamp(-1l)
            .build();

        send(out, Op.WRITE_BLOCK, proto);
    }

    public static void sendPipelineAck(DataOutputStream out, long seqno)
            throws IOException {

        Message proto = DataTransferProtos.PipelineAckProto.newBuilder()
            .setSeqno(seqno)
            .build();

        proto.writeDelimitedTo(out);
    }

    private static void send(DataOutputStream out, Op op, Message proto)
            throws IOException {
        out.writeShort(PROTOCOL_VERSION);
        op.write(out);
        proto.writeDelimitedTo(out);
        out.flush();
    }

    public static Op readOp(DataInputStream in) throws IOException {
        int version = in.readShort();
        if (version != PROTOCOL_VERSION) {
            throw new IOException("invalid data transfer protocol version");
        }

        return Op.read(in);
    }

    public static DataTransferProtos.BlockOpResponseProto
            recvBlockOpResponse(DataInputStream in) throws IOException {
        return DataTransferProtos.BlockOpResponseProto.parseDelimitedFrom(in);
    }

    public static DataTransferProtos.ClientReadStatusProto
            recvClientReadStatus(DataInputStream in) throws IOException {
        return DataTransferProtos.ClientReadStatusProto.parseDelimitedFrom(in);
    }

    public static DataTransferProtos.OpReadBlockProto
            recvReadOp(DataInputStream in) throws IOException {
        return DataTransferProtos.OpReadBlockProto.parseDelimitedFrom(in);
    }

    public static DataTransferProtos.OpWriteBlockProto
            recvWriteOp(DataInputStream in) throws IOException {
        return DataTransferProtos.OpWriteBlockProto.parseDelimitedFrom(in);
    }

    public static DataTransferProtos.PipelineAckProto
            recvPipelineAck(DataInputStream in) throws IOException {
        return DataTransferProtos.PipelineAckProto.parseDelimitedFrom(in);
    }
}
