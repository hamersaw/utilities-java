package com.bushpath.anamnesis.ipc.rpc;

import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;

import com.bushpath.anamnesis.ipc.rpc.packet_handler.PacketHandler;
import com.bushpath.anamnesis.ipc.rpc.packet_handler.RpcPacketHandler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

public class SocketHandler implements Runnable {
    private Socket socket;
    private RpcPacketHandler rpcPacketHandler;
    private Map<Integer, PacketHandler> packetHandlers;
    private SocketContext socketContext;

    public SocketHandler(Socket socket, RpcPacketHandler rpcPacketHandler,
            Map<Integer, PacketHandler> packetHandlers) {
        this.socket = socket;
        this.rpcPacketHandler = rpcPacketHandler;
        this.packetHandlers = packetHandlers;
        this.socketContext = new SocketContext();
    }

    @Override
    public void run() {
        try {
            DataOutputStream out =
                new DataOutputStream(this.socket.getOutputStream());
            DataInputStream in = new DataInputStream(this.socket.getInputStream());

            // read connection header
            byte[] connectionHeaderBuf = new byte[7];
            in.readFully(connectionHeaderBuf);

            // TODO validate connection header (hrpc, verison, auth_protocol)

            while (true) {
                // read total packet length
                int packetLen = in.readInt();

                // parse rpc header
                RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto =
                    RpcHeaderProtos.RpcRequestHeaderProto.parseDelimitedFrom(in);

                int callId = rpcRequestHeaderProto.getCallId();
                if (callId >= 0) { // rpc request
                    this.rpcPacketHandler.handle(in, out,
                        rpcRequestHeaderProto, this.socketContext);
                } else { // should be handled by a registered packet handler
                    if (!this.packetHandlers.containsKey(callId)) {
                        throw new Exception("packet type with callId '" + callId 
                            + "' not supported");
                    }

                    this.packetHandlers.get(callId).handle(in, out,
                        rpcRequestHeaderProto, this.socketContext);
                }
            }
        } catch(EOFException e) {
        } catch(Exception e) {
            e.printStackTrace();
            System.err.println("failed to read rpc request: " + e);
        } finally {
            try {
                this.socket.close();
            } catch(IOException e) {
                System.err.println("failed to close socket: " + e);
            }
        }
    }
}
