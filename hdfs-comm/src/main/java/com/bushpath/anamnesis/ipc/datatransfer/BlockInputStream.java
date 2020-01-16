package com.bushpath.anamnesis.ipc.datatransfer;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;

import com.bushpath.anamnesis.checksum.Checksum;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.InterruptedException;
import java.net.SocketException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockInputStream extends InputStream {
    private final static int CHUNK_PACKET_BUFFER_SIZE = 3;

    private DataInputStream in;
    private DataOutputStream out;
    private Checksum checksum;

    private byte[] buffer;
    private int startIndex, endIndex;
    private BlockingQueue<ChunkPacket> chunkPacketQueue;
    private BlockingQueue<Long> pipelineAckQueue;
    private Thread pipelineAckThread;
    private boolean lastPacketSeen;

    public BlockInputStream(DataInputStream in, DataOutputStream out,
            Checksum checksum) {
        this.in = in;
        this.out = out;
        this.checksum = checksum;

        this.startIndex = 0;
        this.endIndex = 0;
        this.chunkPacketQueue = new ArrayBlockingQueue<>(CHUNK_PACKET_BUFFER_SIZE);
        this.pipelineAckQueue = new ArrayBlockingQueue<>(CHUNK_PACKET_BUFFER_SIZE);
        this.lastPacketSeen = false;

        new ChunkReader().start();
        this.pipelineAckThread = new PipelineAckWriter();
        this.pipelineAckThread.start();
    }

    @Override
    public int read() throws IOException {
        // read packet if no bytes in buffer
        if (this.startIndex == this.endIndex) {
            if (this.readPacket() == 0) {
                throw new EOFException();
            }
        }

        int value = this.buffer[this.startIndex];
        this.startIndex += 1;
        return value;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return this.read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int bytesRead = 0;
        int bIndex = 0;

        while(bytesRead < len) {
            // read packet if no bytes in buffer
            if (this.startIndex == this.endIndex) {
                if (this.readPacket() == 0) {
                    break;
                }
            }

            // copy bytes from this.buffer to b
            int copyLen = Math.min(this.endIndex - this.startIndex, len - bytesRead);
            System.arraycopy(this.buffer, this.startIndex, b, bIndex, copyLen);
            bytesRead += copyLen;
            bIndex += copyLen;
            this.startIndex += copyLen;
        }

        return bytesRead;
    }

    private int readPacket() throws IOException {
        if (this.chunkPacketQueue.isEmpty() && lastPacketSeen) {
            return 0;
        }

        // read next packet
        try {
            ChunkPacket packet = this.chunkPacketQueue.take();

            if (packet.getSequenceNumber() != -1) {
                // refill buffer
                this.buffer = packet.getBuffer();
                this.startIndex = 0;
                this.endIndex = (int) packet.getBufferLength();

                // send ack
                while (!this.pipelineAckQueue.offer(packet.getSequenceNumber())) {};
            }

            // check lastPacketInBlock
            this.lastPacketSeen = this.lastPacketSeen
                || packet.getLastPacketInBlock();

            return this.endIndex;
        } catch(InterruptedException e) {
            throw new IOException("failed to read chunk packet:" + e.toString());
        }
    }

    @Override
    public void close() throws IOException {
        try {
            this.pipelineAckThread.interrupt();
            this.pipelineAckThread.join();
        } catch(InterruptedException e) {
            throw new IOException("failed to join pipeline ack thread:" + e.toString());
        }
    }

    private class ChunkReader extends Thread {
        @Override
        public void run() {
            boolean lastPacketSeen = false;

            // read packets from data input stream
            while (!lastPacketSeen) {
                try {
                    // read packet header
                    int packetLength = in.readInt();
                    short headerLength = in.readShort();
                    byte[] headerBuffer = new byte[headerLength];
                    in.readFully(headerBuffer);

                    DataTransferProtos.PacketHeaderProto packetHeaderProto =
                        DataTransferProtos.PacketHeaderProto.parseFrom(headerBuffer);

                    lastPacketSeen = packetHeaderProto.getLastPacketInBlock();

                    // read checksums
                    byte[] checksumBuffer =
                        new byte[packetLength - 4 - packetHeaderProto.getDataLen()];
                    in.readFully(checksumBuffer);

                    // read data
                    byte[] dataBuffer = new byte[packetHeaderProto.getDataLen()];
                    in.readFully(dataBuffer);

                    // validate checksums
                    byte[] checksums = new byte[checksumBuffer.length];
                    checksum.bulkCompute(dataBuffer, 0, dataBuffer.length, 
                        checksums, 0, checksums.length);
                    for (int i=0; i<checksums.length; i++) {
                        if (checksumBuffer[i] != checksums[i]) {
                            throw new IOException("invalid chunk checksum. expecting "
                                   + checksumBuffer[i] + " and got " + checksums[i]);
                        }
                    }

                    ChunkPacket chunkPacket = new ChunkPacket(
                            packetHeaderProto.getLastPacketInBlock(),
                            packetHeaderProto.getSeqno(),
                            dataBuffer,
                            dataBuffer.length,
                            packetHeaderProto.getOffsetInBlock());

                    while(!chunkPacketQueue.offer(chunkPacket)) {}
                } catch(EOFException | SocketException e) {
                    ChunkPacket chunkPacket = new ChunkPacket(true, -1, null, 0, 0);
                    while(!chunkPacketQueue.offer(chunkPacket)) {}
                    break;
                } catch(Exception e) {
                    System.err.println("ChunkReader failed: " + e.toString());
                    e.printStackTrace();
                    break;
                } 
            }
        }
    }

    private class PipelineAckWriter extends Thread {
        @Override
        public void run() {
            // loop until last packet has been seen
            Long sequenceNumber;
            try {
                while (!lastPacketSeen) {
                    // send pipeline ack
                    sequenceNumber = pipelineAckQueue.poll(50,
                        TimeUnit.MILLISECONDS);
                    if (sequenceNumber == null) {
                        continue;
                    }

                    DataTransferProtocol.sendPipelineAck(out, sequenceNumber);
                }
            } catch (InterruptedException e) {
            } catch (Exception e) {
                System.err.println("PipelineAckWriter failed: " + e.toString());
            }
        }
    }
}
