package com.bushpath.hdfs_comm.ipc.rpc.packet_handler;

import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;

import com.bushpath.hdfs_comm.ipc.rpc.SocketContext;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public interface PacketHandler {
    public abstract int getCallId();
    public abstract void handle(DataInputStream in, DataOutputStream out, 
        RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto,
        SocketContext socketContext) throws Exception;
}
