package com.example;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class MapObject {
    String value;
    long timestamp;

    public MapObject(String value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }
}

public class MyHashMap {
    private Map<String, MapObject> myMap = new ConcurrentHashMap<String, MapObject>();
    
    public int insert(String key, String value, long ts) {
        if (!myMap.containsKey(key) || myMap.get(key).timestamp < ts) {
            MapObject obj = new MapObject(value, ts);
            myMap.put(key, obj);
            return 1;
        }

        return -1;
    }

    public String get(String key) {
        if (myMap.containsKey(key)) {
            return myMap.get(key).value;
        }
        return "NOT FOUND";
    }

    public long getTimestamp(String key) {
        if (myMap.containsKey(key)) {
            return myMap.get(key).timestamp;
        }
        return -1;
    }

    public Set<String> getKeys() {
        return myMap.keySet();
    }

    public void delete(String key) {
        if (myMap.containsKey(key)) {
            myMap.remove(key);
        }
    }
}
