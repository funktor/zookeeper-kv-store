package com.example;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZKClientTest {
    private static ZKClientManagerImpl zkmanager = new ZKClientManagerImpl();
	// ZNode Path
	private String path = "/QN-GBZnode";
	byte[] data = "www.java.globinch.com ZK Client Data".getBytes();

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for
	 * {@link com.globinch.zoo.client.ZKClientManagerImpl#create(java.lang.String, byte[])}
	 * .
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	@Test
	public void testCreate() throws KeeperException, InterruptedException {
		// data in byte array
		
		zkmanager.create(path, data, true, false);
		Stat stat = zkmanager.getZNodeStats(path);
		assertNotNull(stat);
		zkmanager.delete(path);
	}

	/**
	 * Test method for
	 * {@link com.globinch.zoo.client.ZKClientManagerImpl#getZNodeStats(java.lang.String)}
	 * .
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	@Test
	public void testGetZNodeStats() throws KeeperException,
			InterruptedException {
		zkmanager.create(path, data, true, false);
		Stat stat = zkmanager.getZNodeStats(path);
		assertNotNull(stat);
		assertNotNull(stat.getVersion());
		zkmanager.delete(path);

	}

	/**
	 * Test method for
	 * {@link com.globinch.zoo.client.ZKClientManagerImpl#getZNodeData(java.lang.String)}
	 * .
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Test
	public void testGetZNodeData() throws KeeperException, InterruptedException {
		zkmanager.create(path, data, true, false);
		String data = (String)zkmanager.getZNodeData(path,false);
		assertNotNull(data);
		zkmanager.delete(path);
	}

	/**
	 * Test method for
	 * {@link com.globinch.zoo.client.ZKClientManagerImpl#update(java.lang.String, byte[])}
	 * .
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Test
	public void testUpdate() throws KeeperException, InterruptedException {
		zkmanager.create(path, data, true, false);
		String data = "www.java.globinch.com Updated Data";
		byte[] dataBytes = data.getBytes();
		zkmanager.update(path, dataBytes);
		String retrivedData = (String)zkmanager.getZNodeData(path,false);
		assertNotNull(retrivedData);
		zkmanager.delete(path);
	}

	/**
	 * Test method for
	 * {@link com.globinch.zoo.client.ZKClientManagerImpl#getZNodeChildren(java.lang.String)}
	 * .
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Test
	public void testGetZNodeChildren() throws KeeperException, InterruptedException {
		zkmanager.create(path, data, true, false);
		List<String> children= zkmanager.getZNodeChildren(path);
		assertNotNull(children);
		zkmanager.delete(path);
	}

	/**
	 * Test method for
	 * {@link com.globinch.zoo.client.ZKClientManagerImpl#delete(java.lang.String)}
	 * .
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Test
	public void testDelete() throws KeeperException, InterruptedException {
		zkmanager.create(path, data, true, false);
		zkmanager.delete(path);
		Stat stat = zkmanager.getZNodeStats(path);
		assertNull(stat);
	}
}
