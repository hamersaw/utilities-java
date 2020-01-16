package com.bushpath.anamnesis.ipc.datatransfer;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;

import com.bushpath.anamnesis.checksum.Checksum;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

public class BlockOutputStream extends OutputStream {
    private final static int CHUNK_PACKET_BUFFER_SIZE = 3;

    private DataInputStream in;
    private DataOutputStream out;
    private Checksum checksum;

    private byte[] buffer;
    private int index;
    private long sequenceNumber, offsetInBlock;
    private BlockingQueue<ChunkPacket> chunkPacketQueue;
    private BlockingQueue<Long> pipelineAckQueue;
    private Thread chunkWriterThread;

    public BlockOutputStream(DataInputStream in, DataOutputStream out,
            Checksum checksum, long offsetInBlock) {
        this.in = in;
        this.out = out;
        this.checksum = checksum;

        this.buffer = new byte[DataTransferProtocol.CHUNK_SIZE
            * DataTransferProtocol.CHUNKS_PER_PACKET];
        this.index = 0;
        this.sequenceNumber = 0;
        this.offsetInBlock = offsetInBlock;
        this.chunkPacketQueue = new ArrayBlockingQueue<>(CHUNK_PACKET_BUFFER_SIZE);
        this.pipelineAckQueue = new ArrayBlockingQueue<>(CHUNK_PACKET_BUFFER_SIZE);
        this.chunkWriterThread = new ChunkWriter();
        chunkWriterThread.start();
    }

    @Override
    public void write(int b) throws IOException {
        // write packet if buffer is full
        if (this.index == this.buffer.length) {
            this.writePacket(false);
        }

        this.buffer[this.index] = (byte) b;
        this.index += 1;
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int bytesWrote = 0;
        int bIndex = off;

        while (bytesWrote < len) {
            // write packet if buffer is full
            if (this.index == this.buffer.length) {
                this.writePacket(false);
            }

            // copy bytes from b to buffer
            int copyLen = Math.min(this.buffer.length - this.index, len - bytesWrote);
            System.arraycopy(b, bIndex, this.buffer, this.index, copyLen);
            bytesWrote += copyLen;
            bIndex += copyLen;
            this.index += copyLen;
        }
    }

    @Override
    public void close() throws IOException {
        // if buffer is not empty write packet
        if (this.index != 0) {
            writePacket(false);
        }

        // write empty last packet
        writePacket(true);

        // wait until all chunks are written
        try {
            this.chunkWriterThread.join();
        } catch(InterruptedException e) {
            throw new IOException("failed to join pipeline ack thread:" + e.toString());
        }
    }

    private void writePacket(boolean lastPacketInBlock) throws IOException {
        // create chunk packet
        ChunkPacket chunkPacket = new ChunkPacket(lastPacketInBlock,
            this.sequenceNumber, this.buffer, this.index, this.offsetInBlock);

        while (!this.chunkPacketQueue.offer(chunkPacket)) {}

        // update instance variables
        this.sequenceNumber += 1;
        this.offsetInBlock += this.index;
        this.buffer = new byte[DataTransferProtocol.CHUNK_SIZE
            * DataTransferProtocol.CHUNKS_PER_PACKET];
        this.index = 0;
    }

    private class ChunkWriter extends Thread {
        @Override
        public void run() {
            ChunkPacket chunkPacket = null;
            while (true) {
                try {
                    chunkPacket = chunkPacketQueue.take();

                    // compute packet length
                    int checksumCount = (int) Math.ceil(chunkPacket.getBufferLength()
                        / (double) DataTransferProtocol.CHUNK_SIZE);
                    int packetLength = 4 + chunkPacket.getBufferLength()
                        + (checksumCount * 4);
                    out.writeInt(packetLength);

                    // write packet header
                    DataTransferProtos.PacketHeaderProto packetHeaderProto =
                        DataTransferProtos.PacketHeaderProto.newBuilder()
                            .setOffsetInBlock(chunkPacket.getOffsetInBlock())
                            .setSeqno(chunkPacket.getSequenceNumber())
                            .setLastPacketInBlock(chunkPacket.getLastPacketInBlock())
                            .setDataLen(chunkPacket.getBufferLength())
                            .setSyncBlock(false)
                            .build();

                    out.writeShort((short) packetHeaderProto.getSerializedSize());
                    packetHeaderProto.writeTo(out);
             
                    // write checksums
                    byte[] checksums = new byte[checksumCount * 4];
                    checksum.bulkCompute(
                        chunkPacket.getBuffer(), 0, chunkPacket.getBufferLength(), 
                        checksums, 0, checksums.length);
                    out.write(checksums, 0, checksums.length);
             
                    // write data
                    out.write(chunkPacket.getBuffer(), 0, chunkPacket.getBufferLength());
                    out.flush();

                    // if last packet in block break from loop
                    if (chunkPacket.getLastPacketInBlock()) {
                        break;
                    }
                } catch(Exception e) {
                    System.err.println("ChunkWriter failed: " + e.toString());
                    System.err.println(chunkPacket.getSequenceNumber() + ":" + chunkPacket.getLastPacketInBlock());
                    e.printStackTrace();
                    break;
                }
            }
        }
    }
}
