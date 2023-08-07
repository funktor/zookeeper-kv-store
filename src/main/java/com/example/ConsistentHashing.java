package com.example;

import java.util.TreeMap;

import org.apache.commons.codec.digest.MurmurHash3;

import java.util.Map;

public class ConsistentHashing {
    TreeMap<Integer, String> keyValueMap = new TreeMap<Integer, String>();

    public int getHash(String value) {
        return MurmurHash3.hash32x86(value.getBytes());
    }

    public void insert(String value) {
        int key = getHash(value);
        synchronized(this) {
            if (!keyValueMap.containsKey(key)) {
                keyValueMap.put(key, value);
            }
        }
    }

    public int getNextKey(String value, boolean greater) {
        int key = getHash(value);

        synchronized(this) {
            if (!greater && keyValueMap.containsKey(key)) {
                return key;
            }

            Map.Entry<Integer, String> entry = keyValueMap.higherEntry(key);

            if (entry == null) {
                return keyValueMap.firstEntry().getKey();
            }

            return keyValueMap.higherEntry(key).getKey();
        }
    }

    public String getNext(String value, boolean greater) {
        int key = getNextKey(value, greater);
        synchronized(this) {
            return keyValueMap.get(key);
        }
    }

    public void delete(String value) {
        int key = getHash(value);
        synchronized(this) {
            if (keyValueMap.containsKey(key)) {
                keyValueMap.remove(key);
            }
        }
    }

    public void clear() {
        synchronized(this) {
            keyValueMap.clear();
        }
    }

    public boolean exists(String value) {
        int key = getHash(value);
        synchronized(this) {
            return keyValueMap.containsKey(key);
        }
    }

    public void print() {
        for (int k : keyValueMap.keySet()) {
            System.out.println(k + " : " + keyValueMap.get(k));
        }
    }
}
