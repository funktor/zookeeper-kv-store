package com.example;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public interface  ZKManager {
    /**
	 * Create a Znode and save some data
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public int create(String path, byte[] data, boolean isPersistent, boolean isSequential) throws KeeperException, InterruptedException;

    public void createAsync(String path, byte[] data, StringCallback cb, boolean isPersistent, boolean isSequential) throws KeeperException,
			InterruptedException;

	/**
	 * Get the ZNode Stats
	 * 
	 * @param path
	 * @return Stat
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public Stat getZNodeStats(String path) throws KeeperException,
			InterruptedException;

    public void getZNodeStatsAsync(String path, Watcher watcher, StatCallback cb) throws KeeperException,
			InterruptedException;

	/**
	 * Get ZNode Data
	 * 
	 * @param path
	 * @param boolean watchFlag
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String getZNodeData(String path,boolean watchFlag) throws KeeperException,
			InterruptedException;

    public void getZNodeDataAsync(String path, Watcher watcher, DataCallback cb) throws KeeperException,
			InterruptedException;

	/**
	 * Update the ZNode Data
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void update(String path, byte[] data) throws KeeperException,
			InterruptedException;

    public void updateAsync(String path, byte[] data, StatCallback cb) throws KeeperException,
			InterruptedException;

	/**
	 * Get ZNode children
	 * 
	 * @param path
	 * @throws KeeperException
	 * @throws InterruptedException
	 * return List
	 */
	public List<String> getZNodeChildren(String path) throws KeeperException,
			InterruptedException;

    public void getZNodeChildrenAsync(String path, Watcher watcher, ChildrenCallback cb) throws KeeperException,
			InterruptedException;

	/**
	 * Delete the znode
	 * 
	 * @param path
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void delete(String path) throws KeeperException,
			InterruptedException;
}
