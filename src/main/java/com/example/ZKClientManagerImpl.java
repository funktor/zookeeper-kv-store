package com.example;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher;

public class ZKClientManagerImpl implements ZKManager {
    public static ZooKeeper zkeeper;
	public static ZKConnection zkConnection;

	public ZKClientManagerImpl() {
		initialize();
	}

	/**
	 * Initialize connection
	 */
	private void initialize() {
		try {
			zkConnection = new ZKConnection();
			zkeeper = zkConnection.connect("localhost");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	/**
	 * Close the zookeeper connection
	 */
	public void closeConnection() {
		try {
			zkConnection.close();
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Override
	public int create(String path, byte[] data, boolean isPersistent, boolean isSequential) throws KeeperException,
			InterruptedException {
        
        Stat stat = getZNodeStats(path);

        if (stat == null) {
            if (isPersistent) {
                zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				    CreateMode.PERSISTENT);
            }
            else {
                if (isSequential) {
                    zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				        CreateMode.EPHEMERAL_SEQUENTIAL);
                }
                else {
                    zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				        CreateMode.EPHEMERAL);
                }
            }
            return 1;
        }

        return -1;
	}

    @Override
	public void createAsync(String path, byte[] data, StringCallback cb, boolean isPersistent, boolean isSequential) throws KeeperException,
			InterruptedException {
        
        if (isPersistent) {
            zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, cb, null);
        }
        else {
            if (isSequential) {
                zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL, cb, null);
            }
            else {
                zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL, cb, null);
            }
        }
	}

	@Override
	public Stat getZNodeStats(String path) throws KeeperException,
			InterruptedException {
		Stat stat = zkeeper.exists(path, true);
		if (stat != null) {
			System.out.println("Node exists and the node version is "
					+ stat.getVersion());
		} else {
			System.out.println("getZNodeStats Node does not exists : " + path);
		}
		return stat;
	}

    @Override
	public void getZNodeStatsAsync(String path, Watcher watcher, StatCallback cb) throws KeeperException,
			InterruptedException {
        zkeeper.exists(path, watcher, cb, null);
	}

	@Override
	public String getZNodeData(String path, boolean watchFlag) throws KeeperException,
			InterruptedException {
		try {
			Stat stat = getZNodeStats(path);
			byte[] b = null;

			if (stat != null) {
				if(watchFlag){
					ZKWatcher watch = new ZKWatcher();
					b = zkeeper.getData(path, watch,null);
					watch.await();
				}else{
					b = zkeeper.getData(path, null, null);
				}

				String data = new String(b, "UTF-8");
				System.out.println(data);
				
				return data;
			} else {
				System.out.println("getZNodeData Node does not exists : " + path);
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return null;
	}

    @Override
	public void getZNodeDataAsync(String path, Watcher watcher, DataCallback cb) throws KeeperException,
			InterruptedException {
        zkeeper.getData(path, watcher, cb, null);
	}

	@Override
	public void update(String path, byte[] data) throws KeeperException,
			InterruptedException {
		int version = zkeeper.exists(path, true).getVersion();        
        zkeeper.setData(path, data, version);
    }

    @Override
	public void updateAsync(String path, byte[] data, StatCallback cb) throws KeeperException,
			InterruptedException {
        Stat stat = getZNodeStats(path);
        zkeeper.setData(path, data, stat.getVersion(), cb, null);
    }

	@Override
	public List<String> getZNodeChildren(String path) throws KeeperException,
			InterruptedException {
		Stat stat = getZNodeStats(path);
		List<String> children  = null;

		if (stat != null) {
			children = zkeeper.getChildren(path, false);
			for (int i = 0; i < children.size(); i++)
				System.out.println(children.get(i)); 
		} else {
			System.out.println("getZNodeChildren Node does not exists : " + path);
		}
		return children;
	}

    @Override
	public void getZNodeChildrenAsync(String path, Watcher watcher, ChildrenCallback cb) throws KeeperException,
			InterruptedException {
        zkeeper.getChildren(path, watcher, cb, null);
	}

	@Override
	public void delete(String path) throws KeeperException,
			InterruptedException {
		int version = zkeeper.exists(path, true).getVersion();
		zkeeper.delete(path, version);
	}
}
