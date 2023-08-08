package com.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Gateway {
    private static String hostPort;
    private static Selector selector;
    private static ServerSocketChannel serverSocket;
    private static Controller ctl;

    public static void main(String[] args) throws IOException, InterruptedException {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        hostPort = host + ":" + String.valueOf(port);

        selector = Selector.open();
        ctl = new Controller(hostPort, "0", selector);

        new Thread(() -> ctl.checkPartitionsAndAddLeaders()).start();
        
        serverSocket = ServerSocketChannel.open();

        serverSocket.bind(new InetSocketAddress("localhost", port));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);

        runServer();
    }

    public static void runServer() {
        while (true) {
            try {
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter = selectedKeys.iterator();
                
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();

                    if (key.isAcceptable()) {
                        SocketChannel client = serverSocket.accept();
                        ctl.registerClientWithSelector(selector, client);
                    }

                    if (key.isReadable()) {
                        SocketChannel client = (SocketChannel) key.channel();
                        List<String> msgs = ctl.getMessages(client);

                        for (String msg : msgs) {
                            HandleRequest handler = new HandleRequest(msg, client, ctl);
                            handler.handleGatewayRequest();
                        }
                    }

                    iter.remove();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
