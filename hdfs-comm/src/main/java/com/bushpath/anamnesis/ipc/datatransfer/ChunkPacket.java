package com.bushpath.anamnesis.ipc.datatransfer;

public class ChunkPacket {
    private boolean lastPacketInBlock;
    private long sequenceNumber;
    private byte[] buffer;
    private int bufferLength;
    private long offsetInBlock;

    public ChunkPacket(boolean lastPacketInBlock, long sequenceNumber,
            byte[] buffer, int bufferLength, long offsetInBlock) {
        this.lastPacketInBlock = lastPacketInBlock;
        this.sequenceNumber = sequenceNumber;
        this.buffer = buffer;
        this.bufferLength = bufferLength;
        this.offsetInBlock = offsetInBlock;
    }

    public boolean getLastPacketInBlock() {
        return this.lastPacketInBlock;
    }

    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    public byte[] getBuffer() {
        return this.buffer;
    }

    public int getBufferLength() {
        return this.bufferLength;
    }

    public long getOffsetInBlock() {
        return this.offsetInBlock;
    }
}
