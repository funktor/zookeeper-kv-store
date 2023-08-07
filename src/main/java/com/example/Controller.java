package com.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

public class Controller {
    private String hostPort;
    private String partitionId;
    private MyHashMap myMap = new MyHashMap();
    private ZKClientManagerImpl zkmanager = new ZKClientManagerImpl();
    private ConsistentHashing partitioner = new ConsistentHashing();
    private Set<String> replicas = new ConcurrentSkipListSet<String>();
    private CommitLog commitLog;
    private Map<String, Integer> logSeq = new ConcurrentHashMap<String, Integer>();
    private boolean isLeader = false;
    private CommonUtils utils = new CommonUtils();
    private Map<String, SocketChannel> nodeMap = new ConcurrentHashMap<String, SocketChannel>();
    private Selector selector;
    private String DELIM = "<EOM>";
    private Map<String, SocketChannel> requestMap = new ConcurrentHashMap<String, SocketChannel>();

    public Controller(String hostPort, String partitionId, Selector selector) {
        this.hostPort = hostPort;
        this.partitionId = partitionId;
        this.selector = selector;
        commitLog = new CommitLog("commitlog-" + hostPort + ".txt");
    }

    public MyHashMap getHashMap() {
        return myMap;
    }

    public ConsistentHashing getPartitioner() {
        return partitioner;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    public String getHostPort() {
        return hostPort;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void createZnodes() {
        try {
            byte[] data = "Hello".getBytes();

            zkmanager.create("/partitions", data, true, false);
            zkmanager.create("/partitions/" + partitionId, data, true, false);
            zkmanager.create("/partitions/" + partitionId + "/replicas", data, true, false);
            zkmanager.create("/partitions/" + partitionId + "/replicas/" + hostPort, "-1".getBytes(), false, false);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addNodeToPartitioner() {
        try {
            partitioner.insert(partitionId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addNodeToReplicas() {
        try {
            replicas.add(hostPort);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void writeLog(String msg) {
        try {
            commitLog.log(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<String> getLogs(int start) {
        try {
            return commitLog.readLines(start);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public List<String> getMessages(SocketChannel client) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        String remainder = "";
        List<String> all_msgs = new ArrayList<String>();

        while(true) {
            int r = client.read(buffer);

            if (r > 0) {
                String msg = new String(buffer.array(), 0, r);

                msg = remainder + msg;
                MessageParsedTuple parsedTuple = utils.split(msg, DELIM);

                List<String> parts = parsedTuple.parts;
                msg = parsedTuple.finalString;

                all_msgs.addAll(parts);

                remainder = msg;
                buffer.clear();
            }
            else if (r == 0) {
                break;
            }
            else {
                client.close();
                System.out.println("Not accepting client messages anymore");
                break;
            }
        }

        return all_msgs;
    }

    public void registerClientWithSelector(Selector selector, SocketChannel client) {
        try {
            client.configureBlocking(false);
            client.register(selector, SelectionKey.OP_READ);
        } catch (Exception e) {
            e.printStackTrace();
        } 
        
    }

    public void sendMessage(String request, String nodeHostPort) {
        try {
            SocketChannel socket;

            if (nodeMap.containsKey(nodeHostPort)) {
                socket = nodeMap.get(nodeHostPort);
            }
            else {
                String[] ipPort = nodeHostPort.split(":");
                String ip = ipPort[0];
                int port = Integer.parseInt(ipPort[1]);
                socket = SocketChannel.open(new InetSocketAddress(ip, port));
                registerClientWithSelector(selector, socket);
                nodeMap.put(nodeHostPort, socket);
            }

            ByteBuffer buffer = ByteBuffer.wrap(request.getBytes());
            
            while(true) {
                int m = socket.write(buffer);

                if (m == -1) {
                    socket.close();

                    TimeUnit.SECONDS.sleep(1);

                    String[] ipPort = nodeHostPort.split(":");
                    String ip = ipPort[0];
                    int port = Integer.parseInt(ipPort[1]);
                    socket = SocketChannel.open(new InetSocketAddress(ip, port));
                    registerClientWithSelector(selector, socket);
                    nodeMap.put(nodeHostPort, socket);
                }
                else {
                    break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } 
    }

    public void sendMessage(String request, SocketChannel socket) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(request.getBytes());
            socket.write(buffer);

        } catch (Exception e) {
            e.printStackTrace();
        } 
    }

    public void updateSequence(int sequence) {
        try {
            zkmanager.updateAsync(
                "/partitions/" + partitionId + "/replicas/" + hostPort, 
                Integer.toString(sequence).getBytes(), 
                new StatCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, Stat stat) {
                        switch(Code.get(rc)) {
                            case OK:
                                break;
                            default:
                                updateSequence(sequence);
                                break;
                        }
                    }
                });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getLeaderForPartition(String pid) {
        String leader = null;
        try {
            leader = zkmanager.getZNodeData("/partitions/" + pid + "/leader", false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return leader;
    }

    public void addPartitions() {
        try {
            zkmanager.getZNodeChildrenAsync(
                "/partitions", 
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getType() == EventType.NodeChildrenChanged) {
                            addPartitions();
                        }
                    }
                }, 
                new ChildrenCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, List<String> children) {
                        switch(Code.get(rc)) {
                            case OK:
                                for (String partition : children) {
                                    if (!partition.equals(partitionId)) {
                                        System.out.println("New Partition : " + partition);
                                        partitioner.insert(partition);
                                    }
                                }
                                    
                                break;
                            case CONNECTIONLOSS:
                                addPartitions();
                                break;
                            default:
                                break;
                        }
                    }
                }
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getLogSequence(String partitionId, String replica) {
        try {
            String path = "/partitions/" + partitionId + "/replicas/" + replica;
    
            zkmanager.getZNodeDataAsync(
                path, 
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getType() == EventType.NodeDataChanged) {
                            getLogSequence(partitionId, replica);
                        }
                    }
                }, 
                new DataCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                        switch(Code.get(rc)) {
                            case OK:
                                try {
                                    String d = new String(data, "UTF-8");

                                    System.out.println("Log sequence : " + replica + "=" + d);
                                    logSeq.put(replica, Integer.parseInt(d));

                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                break;

                            case NONODE:
                                break;

                            case CONNECTIONLOSS:
                                getLogSequence(partitionId, replica);
                                break;

                            default:
                                break;
                        }
                    }
                }
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addReplicas() {
        try {
            zkmanager.getZNodeChildrenAsync(
                "/partitions/" + partitionId + "/replicas", 
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getType() == EventType.NodeChildrenChanged) {
                            addReplicas();
                        }
                    }
                }, 
                new ChildrenCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, List<String> children) {
                        switch(Code.get(rc)) {
                            case OK:
                                replicas = new ConcurrentSkipListSet<String>(children);
                                logSeq.clear();

                                for (String replica : replicas) {
                                    System.out.println("New replica : " + replica);
                                    getLogSequence(partitionId, replica);
                                }

                                break;

                            case CONNECTIONLOSS:
                                addReplicas();
                                break;

                            default:
                                break;
                        }
                    }
                }
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runForLeader() {
        try {
            int maxSeq = Integer.MIN_VALUE;
            String leader = null;

            for (String replica : logSeq.keySet()) {
                int seqs = logSeq.get(replica);

                if ((seqs > maxSeq) 
                    || (seqs == maxSeq 
                        && leader != null 
                        && replica.compareTo(leader) < 0)) {

                    maxSeq = seqs;
                    leader = replica;
                }
            }

            if (leader != null && leader.equals(hostPort)) {
                zkmanager.createAsync(
                    "/partitions/" + partitionId + "/leader", 
                    leader.getBytes(), 
                    new StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, String name) {
                            switch(Code.get(rc)) {
                                case CONNECTIONLOSS:
                                    runForLeader();
                                    break;
                                case OK:
                                    System.out.println("New Leader : " + hostPort);
                                    isLeader = true;
                                    break;
                                case NODEEXISTS:
                                    leaderExists();
                                    break;
                                default:
                                    break;
                            }
                        }
                    }, 
                    false, 
                    false);
            }
            else {
                leaderExists();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void leaderExists() {
        try {
            String path = "/partitions/" + partitionId + "/leader";
    
            zkmanager.getZNodeStatsAsync(
                path, 
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getType() == EventType.NodeDeleted) {
                            runForLeader();
                        }
                    }
                },
                new StatCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, Stat stat) {
                        switch(Code.get(rc)) {
                            case NONODE:
                                runForLeader();
                                break;
                            case CONNECTIONLOSS:
                                leaderExists();
                                break;
                            case NODEEXISTS:
                                leaderExists();
                                break;
                            default:
                                break;
                        }
                    }
                }
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void reconcileKeys() {
        try {
            if (isLeader) {
                String partition = partitioner.getNext(partitionId, true);

                if (!partition.equals(partitionId)) {
                    String nextNode = getLeaderForPartition(partition);

                    JSONObject jsonObj = new JSONObject();
                    UUID uuid = UUID.randomUUID();

                    jsonObj.put("operator", "RECONCILE-KEYS");
                    jsonObj.put("request_id", uuid.toString());

                    JSONObject dataObj = new JSONObject();
                    dataObj.put("partition", partitionId);

                    jsonObj.put("data", dataObj);
                    jsonObj.put("request_type", 0);
                    jsonObj.put("timestamp", System.currentTimeMillis());

                    sendMessage(jsonObj.toString() + "<EOM>", nextNode);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runReconciliation() {
        try {
            while(true) {
                reconcileKeys();
                TimeUnit.SECONDS.sleep(5);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void replicate() {
        try {
            while(true) {
                if (isLeader) {
                    for (String replica : replicas) {
                        if (!replica.equals(hostPort)) {
                            int seq = -1;
                            seq = logSeq.get(replica);
                            
                            List<String> logsToSend = getLogs(seq+1);

                            String msg = "";
                            int s = seq+1;

                            for (String log : logsToSend) {
                                JSONObject jObj = new JSONObject();
                                jObj.put("operator", "REPLICATE");

                                JSONObject dataObj = new JSONObject();
                                dataObj.put("sequence", s);
                                dataObj.put("data", log);

                                jObj.put("data", dataObj);

                                UUID uuid = UUID.randomUUID();

                                jObj.put("request_id", uuid.toString());
                                jObj.put("request_type", 0);
                                jObj.put("timestamp", System.currentTimeMillis());

                                msg += jObj.toString() + "<EOM>";
                                s += 1;
                            }

                            sendMessage(msg, replica);
                        }
                    }
                }

                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Map<String, SocketChannel> getRequestMap() {
        return requestMap;
    }
}
