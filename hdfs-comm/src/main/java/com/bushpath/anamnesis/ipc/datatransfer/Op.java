package com.bushpath.anamnesis.ipc.datatransfer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum Op {
    WRITE_BLOCK((byte)80),
    READ_BLOCK((byte)81),
    WRITE_BLOCK_STATS((byte)82);

    public final byte code;

    Op(byte code) {
        this.code = code;
    }

    private static final int FIRST_CODE = values()[0].code;
    private static Op valueOf(byte code) {
        final int i = (code & 0xff) - FIRST_CODE;
        return i < 0 || i >= values().length ? null : values()[i];
    }

    public static Op read(DataInput in) throws IOException {
        return valueOf(in.readByte());
    }

    public void write(DataOutput out) throws IOException {
        out.write(this.code);
    }
}
