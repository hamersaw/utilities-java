package com.bushpath.anamnesis.ipc.rpc;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

import java.io.DataOutputStream;

public class RpcUtil {
    public static void sendMessages(DataOutputStream out, Message... messages) 
            throws Exception {
        // compute length of non-null messages
        int length = 0;
        for (Message message: messages) {
            if (message != null) {
                int l = message.getSerializedSize();
                length += l + CodedOutputStream.computeRawVarint32Size(l);
            }
        }

        // write total packet length
        out.writeInt(length);

        // write non-null messages
        for (Message message: messages) {
            if (message != null) {
                message.writeDelimitedTo(out);
            }
        }

        out.flush();
    }
}
