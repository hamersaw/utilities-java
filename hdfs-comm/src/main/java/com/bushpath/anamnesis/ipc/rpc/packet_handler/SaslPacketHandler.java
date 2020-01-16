package com.bushpath.anamnesis.ipc.rpc.packet_handler;

import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;

import com.bushpath.anamnesis.ipc.rpc.RpcUtil;
import com.bushpath.anamnesis.ipc.rpc.SocketContext;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public class SaslPacketHandler implements PacketHandler {
    @Override
    public int getCallId() {
        return -33;
    }

    @Override
    public void handle(DataInputStream in, DataOutputStream out, 
            RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto,
            SocketContext socketContext) throws Exception {
        // handle SASL RPC request
        RpcHeaderProtos.RpcSaslProto rpcSaslProto =
            RpcHeaderProtos.RpcSaslProto.parseDelimitedFrom(in);

        switch (rpcSaslProto.getState()) {
        case NEGOTIATE:
            // send automatic SUCCESS - mean simple server on HDFS side
            RpcHeaderProtos.RpcResponseHeaderProto rpcResponse =
                RpcHeaderProtos.RpcResponseHeaderProto.newBuilder()
                    .setStatus(RpcStatusProto.SUCCESS)
                    .setCallId(rpcRequestHeaderProto.getCallId())
                    .setClientId(rpcRequestHeaderProto.getClientId())
                    .build();

            RpcHeaderProtos.RpcSaslProto message =
                RpcHeaderProtos.RpcSaslProto.newBuilder()
                    .setState(RpcHeaderProtos.RpcSaslProto.SaslState.SUCCESS)
                    .build();

            // send response
            RpcUtil.sendMessages(out, rpcResponse, message);
            break;
        default:
            System.err.println("TODO - handle sasl " + rpcSaslProto.getState());
            break;
        }
    }
}
