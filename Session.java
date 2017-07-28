package com.coreos.jetcd.concurrency;

import java.util.concurrent.atomic.AtomicReference;

import com.coreos.jetcd.Client;

/**
 * @author odl
 *
 */
public class Session {
	private AtomicReference<Client> etcdclient;
	private long lease;
	private int ttl;
	
	public Session(long lease, AtomicReference<Client> client,int ttl){
		this(lease,client);
		this.ttl=ttl;
	}
	
	public Session(long lease, AtomicReference<Client> client) {
		this.lease = lease;
		this.etcdclient = client;
	}
	
	public static Session New(long lease, AtomicReference<Client> client) {
		return new Session(lease,client);
	}
	
	public static Session New(long lease, AtomicReference<Client> client, int ttl) {
		return new Session(lease, client, ttl);
	}
	
	public Client getClient(){
		return etcdclient.get();
	}
	
	public long getLease() {
		return this.lease;
	}
}
