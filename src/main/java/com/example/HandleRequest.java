package com.example;

import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

class MessageFormat {
    public String operator;
    public JSONObject data;
    public String request_id;
    public int request_type;
    public long timestamp;
}

public class HandleRequest {
    private String request;
    private SocketChannel client;
    private Controller ctl;
    private MessageFormat msg; 
    private int NUM_RETRIES=5;

    public HandleRequest(String request, SocketChannel client, Controller ctl) {
        this.request = request;
        this.client = client;
        this.ctl = ctl;
        msg = new MessageFormat();

        JSONObject obj = new JSONObject(request);

        msg.operator = obj.getString("operator");
        msg.data = obj.getJSONObject("data");
        msg.request_id = obj.getString("request_id");
        msg.timestamp = obj.getLong("timestamp");
        msg.request_type = obj.getInt("request_type");
    }

    public void handlePutRequestSelf(
                            String key, 
                            String val, 
                            long timestamp,
                            int logSequence) {

        CommitLog commitLog = ctl.getCommitLog();
        MyHashMap myMap = ctl.getHashMap();

        if (logSequence == commitLog.getSequence() + 1) {
            ctl.writeLog(request);
            ctl.updateSequence(commitLog.getSequence());
            myMap.insert(key, val, timestamp);
        }

        JSONObject obj = new JSONObject(request);

        obj.put("request_type", 1);

        JSONObject dataObject = new JSONObject();
        dataObject.put("status", "OK");
        dataObject.put("node", ctl.getHostPort());

        obj.put("data", dataObject);

        String clientMsg = obj.toString() + "<EOM>";
        ctl.sendMessage(clientMsg, client);
    }

    public void handlePutRequest() {
        String key = msg.data.getString("key");
        String val = msg.data.getString("val");

        ConsistentHashing partitioner = ctl.getPartitioner();
        CommitLog commitLog = ctl.getCommitLog();

        String partition = partitioner.getNext(key, false);

        if (partition.equals(ctl.getPartitionId())) {
            handlePutRequestSelf(
                    key, 
                    val, 
                    msg.timestamp, 
                    commitLog.getSequence() + 1
            );
        } 
        else {
            String node = ctl.getLeaderForPartition(partition);
            
            ctl.getRequestMap().put(msg.request_id, client);
            ctl.sendMessage(request + "<EOM>", node);
        }
    }

    public void handleDelRequestSelf(
                            String key, 
                            int logSequence) {

        CommitLog commitLog = ctl.getCommitLog();
        MyHashMap myMap = ctl.getHashMap();

        if (logSequence == commitLog.getSequence() + 1) {
            ctl.writeLog(request);
            ctl.updateSequence(commitLog.getSequence());
            myMap.delete(key);
        }

        JSONObject obj = new JSONObject(request);

        obj.put("request_type", 1);

        JSONObject dataObject = new JSONObject();
        dataObject.put("status", "OK");
        dataObject.put("node", ctl.getHostPort());
        
        obj.put("data", dataObject);

        String clientMsg = obj.toString() + "<EOM>";
        ctl.sendMessage(clientMsg, client);
    }

    public void handleDeleteRequest() {
        String key = msg.data.getString("key");

        ConsistentHashing partitioner = ctl.getPartitioner();
        CommitLog commitLog = ctl.getCommitLog();

        String partition = partitioner.getNext(key, false);

        if (partition.equals(ctl.getPartitionId())) {
            handleDelRequestSelf(
                    key, 
                    commitLog.getSequence() + 1
            );
        } 
        else {
            String node = ctl.getLeaderForPartition(partition);

            ctl.getRequestMap().put(msg.request_id, client);
            ctl.sendMessage(request + "<EOM>", node);
        }
    }

    public void handleGetRequestSelf(String key) {
        MyHashMap myMap = ctl.getHashMap();
        JSONObject obj = new JSONObject(request);

        obj.put("request_type", 1);

        JSONObject dataObject = new JSONObject();
        dataObject.put("val", myMap.get(key));
        dataObject.put("status", "OK");
        dataObject.put("node", ctl.getHostPort());
        
        obj.put("data", dataObject);

        String clientMsg = obj.toString() + "<EOM>";
        ctl.sendMessage(clientMsg, client);
    }

    public void handleGetRequest() {
        String key = msg.data.getString("key");

        ConsistentHashing partitioner = ctl.getPartitioner();
        String partition = partitioner.getNext(key, false);

        if (partition.equals(ctl.getPartitionId())) {
            handleGetRequestSelf(key);
        } 
        else {
            String node = ctl.getLeaderForPartition(partition);

            ctl.getRequestMap().put(msg.request_id, client);
            ctl.sendMessage(request + "<EOM>", node);
        }
    }

    public void handleReplicateRequest() {
        int sequence = msg.data.getInt("sequence");
        String payload = msg.data.getString("data");

        HandleRequest handler = new HandleRequest(payload, client, ctl);
        String key;

        switch(handler.msg.operator) {
            case "PUT":
                key = handler.msg.data.getString("key");
                String val = handler.msg.data.getString("val");

                handler.handlePutRequestSelf(key, val, handler.msg.timestamp, sequence);
                break;

            case "DEL":
                key = handler.msg.data.getString("key");
                handler.handleDelRequestSelf(key, sequence);
                break;
            
            default:
                break;
        }
    }

    public void handleReconcileKeysRequest() {
        try {
            ConsistentHashing partitioner = ctl.getPartitioner();
            MyHashMap myMap = ctl.getHashMap();

            String partition = msg.data.getString("partition");
            int nodeHash = partitioner.getHash(partition);

            Set<String> keys = myMap.getKeys();
            Set<String> toDelete = new HashSet<String>();

            for (String k : keys) {
                if (partitioner.getNextKey(k, false) == nodeHash) {
                    toDelete.add(k);
                }
            }

            String putResponse = "";

            for (String s :  toDelete) {
                JSONObject jsonObj = new JSONObject();

                UUID uuid = UUID.randomUUID();
                String val = myMap.get(s);
                long ts = myMap.getTimestamp(s);

                JSONObject dataObj = new JSONObject();
                dataObj.put("key", s);
                dataObj.put("val", val);

                jsonObj.put("operator", "PUT");
                jsonObj.put("request_id", uuid.toString());
                jsonObj.put("data", dataObj);
                jsonObj.put("request_type", 0);
                jsonObj.put("timestamp", ts);

                putResponse += jsonObj.toString() + "<EOM>";

                jsonObj = new JSONObject();

                uuid = UUID.randomUUID();

                dataObj = new JSONObject();
                dataObj.put("key", s);

                jsonObj.put("operator", "DEL");
                jsonObj.put("request_id", uuid.toString());
                jsonObj.put("data", dataObj);
                jsonObj.put("request_type", 0);
                jsonObj.put("timestamp", System.currentTimeMillis());

                String req = jsonObj.toString();

                CommitLog commitLog = ctl.getCommitLog();

                HandleRequest handler = new HandleRequest(req, client, ctl);
                handler.handleDelRequestSelf(s, commitLog.getSequence() + 1);
            }

            ctl.sendMessage(putResponse, client);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void handleRequest() {
        System.out.println(request);

        try {
            if (msg.request_type == 1) {
                if (ctl.getRequestMap().containsKey(msg.request_id)) {
                    SocketChannel cl = ctl.getRequestMap().remove(msg.request_id);
                    String clientMsg = request + "<EOM>";
                    ctl.sendMessage(clientMsg, cl);
                }
            }
            else {
                if (msg.operator.equals("PUT")) {
                    handlePutRequest();
                }

                else if (msg.operator.equals("DEL")) {
                    handleDeleteRequest();
                }

                else if (msg.operator.equals("GET")) {
                    handleGetRequest();
                }

                else if (msg.operator.equals("RECONCILE-KEYS")) {
                    new Thread(() -> handleReconcileKeysRequest()).start();
                }

                else if (msg.operator.equals("REPLICATE")) {
                    handleReplicateRequest();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void handleGatewayRequest() {
        System.out.println(request);

        try {
            if (msg.request_type == 1) {
                if (ctl.getRequestMap().containsKey(msg.request_id)) {
                    SocketChannel cl = ctl.getRequestMap().remove(msg.request_id);
                    String clientMsg = request + "<EOM>";
                    ctl.sendMessage(clientMsg, cl);
                }
            }
            else {
                int cnt = 0;
                while (cnt < NUM_RETRIES) {
                    Map<String, String> leaderNodes = ctl.getLeaders();
                    String node = leaderNodes.entrySet().iterator().next().getValue();
                    System.out.println(node);

                    ctl.getRequestMap().put(msg.request_id, client);
                    int resp = ctl.sendMessage(request + "<EOM>", node);
                    if (resp > 0) {
                        break;
                    }
                    cnt += 1;
                    TimeUnit.MILLISECONDS.sleep(100);
                }
                
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
