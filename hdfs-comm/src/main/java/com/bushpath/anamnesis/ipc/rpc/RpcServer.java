package com.bushpath.anamnesis.ipc.rpc;

import com.bushpath.anamnesis.ipc.rpc.packet_handler.PacketHandler;
import com.bushpath.anamnesis.ipc.rpc.packet_handler.RpcPacketHandler;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class RpcServer extends Thread {
    private ServerSocket serverSocket;
    private ExecutorService executorService;
    private RpcPacketHandler rpcPacketHandler;
    private Map<Integer, PacketHandler> packetHandlers;

    public RpcServer(ServerSocket serverSocket,
            ExecutorService executorService) {
        this.serverSocket = serverSocket;
        this.executorService = executorService;
        this.rpcPacketHandler = new RpcPacketHandler();
        this.packetHandlers = new HashMap<>();
    }

    public void addRpcProtocol(String className, Object protocol) {
        this.rpcPacketHandler.addProtocol(className, protocol);
    }

    public void addPacketHandler(PacketHandler packetHandler) {
        this.packetHandlers.put(packetHandler.getCallId(), packetHandler);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = this.serverSocket.accept();

                // add SocketHandler to threadpool
                Runnable socketHandler =  new SocketHandler(socket,
                    this.rpcPacketHandler, this.packetHandlers);
                this.executorService.execute(socketHandler);
            } catch(Exception e) {
                System.err.println("failed to accept server connection: " + e);
            }
        }
    }
}
