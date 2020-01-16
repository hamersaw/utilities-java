package com.bushpath.anamnesis.ipc.rpc.packet_handler;

import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;

import com.bushpath.anamnesis.ipc.rpc.RpcUtil;
import com.bushpath.anamnesis.ipc.rpc.SocketContext;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public class IpcConnectionContextPacketHandler implements PacketHandler {
    @Override
    public int getCallId() {
        return -3;
    }

    @Override
    public void handle(DataInputStream in, DataOutputStream out, 
            RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto,
            SocketContext socketContext) throws Exception {
        IpcConnectionContextProtos.IpcConnectionContextProto context =
            IpcConnectionContextProtos.IpcConnectionContextProto
                .parseDelimitedFrom(in);

        // update socket context
        socketContext.setEffectiveUser(context.getUserInfo().getEffectiveUser());
        socketContext.setRealUser(context.getUserInfo().getRealUser());
        socketContext.setProtocol(context.getProtocol());
    }
}
