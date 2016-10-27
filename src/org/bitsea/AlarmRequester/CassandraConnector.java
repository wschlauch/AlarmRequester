package org.bitsea.AlarmRequester;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;

class CassandraConnector {

	private Cluster cluster;
	private Session session;
	private CodecRegistry codecRegistry;
	/*
	 * Connect to Cassandra cluster specified by node ip and port number
	 */
	public void connect(final String node, final int port, final String keyspace) {
		this.codecRegistry = new CodecRegistry();
		this.cluster = Cluster.builder().addContactPoint(node).withCodecRegistry(codecRegistry)
				.withPort(port).build();
		this.session = cluster.connect(keyspace);
	}
	
	/* 
	 * provide Session
	 */
	public Session getSession() {
		return this.session;
	}
	
	
	public Cluster getCluster() {
		return this.cluster;
	}
	
	/*
	 * close Cluster
	 */
	public void close() {
		this.cluster.close();
	}
	 
	
	public CodecRegistry getRegistry() {
		return this.codecRegistry;
	}
}