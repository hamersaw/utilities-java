package com.bushpath.anamnesis.ipc.rpc.packet_handler;

import com.google.protobuf.Message;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos;

import com.bushpath.anamnesis.ipc.rpc.RpcUtil;
import com.bushpath.anamnesis.ipc.rpc.SocketContext;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class RpcPacketHandler implements PacketHandler {
    private Map<String, Object> protocols;

    public RpcPacketHandler() {
        this.protocols = new HashMap<>();
    }

    public void addProtocol(String className, Object protocol) {
        this.protocols.put(className, protocol);
    }

    @Override
    public int getCallId() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void handle(DataInputStream in, DataOutputStream out, 
            RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto,
            SocketContext socketContext) throws Exception {
        ProtobufRpcEngineProtos.RequestHeaderProto requestHeaderProto =
            ProtobufRpcEngineProtos.RequestHeaderProto.parseDelimitedFrom(in);

        // build response - set to 'SUCCESS' and change on failure
        RpcHeaderProtos.RpcResponseHeaderProto.Builder respBuilder =
            RpcHeaderProtos.RpcResponseHeaderProto.newBuilder()
                .setStatus(RpcStatusProto.SUCCESS)
                .setCallId(rpcRequestHeaderProto.getCallId())
                .setClientId(rpcRequestHeaderProto.getClientId());

        // retreive rpc arguments
        String methodName = requestHeaderProto.getMethodName();
        String declaringClassProtocolName =
            requestHeaderProto.getDeclaringClassProtocolName();

        Message message = null;
        if (!this.protocols.containsKey(declaringClassProtocolName)) {
            // error -> protocol does not exist
            respBuilder.setStatus(RpcStatusProto.ERROR);
            respBuilder.setErrorMsg("protocol '" + declaringClassProtocolName 
                + "' does not exist");
            respBuilder.setErrorDetail(RpcErrorCodeProto.ERROR_NO_SUCH_PROTOCOL);
        } else {
            Object rpcHandler = this.protocols.get(declaringClassProtocolName);

            // check if handler contains method
            Method method = null;
            for (Method m: rpcHandler.getClass().getMethods()) {
                if (m.getName().equals(methodName)) {
                    method = m;
                    break;
                }
            }

            if (method == null) {
                System.out.println("ERROR: method " + declaringClassProtocolName + "."
                    + methodName + " does not exist");

                // error -> method does not exist
                respBuilder.setStatus(RpcStatusProto.ERROR);
                respBuilder.setErrorMsg("method '" + methodName + "' does not exist");
                respBuilder.setErrorDetail(RpcErrorCodeProto.ERROR_NO_SUCH_PROTOCOL);
            } else {
                // use rpc handler to execute method
                try {
                    message = (Message) method.invoke(rpcHandler, in, socketContext);
                } catch(Exception e) {
                    e.printStackTrace();
                    respBuilder.setStatus(RpcStatusProto.ERROR);
                    respBuilder.setExceptionClassName(e.getClass().toString());
                    respBuilder.setErrorMsg(e.toString());
                    respBuilder.setErrorDetail(RpcErrorCodeProto.ERROR_RPC_SERVER);
                }
            }
        }

        RpcHeaderProtos.RpcResponseHeaderProto resp = respBuilder.build();

        // send response
        RpcUtil.sendMessages(out, resp, message);
    }
}
