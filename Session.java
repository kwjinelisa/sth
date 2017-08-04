package com.coreos.jetcd.concurrency;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Lease.KeepAliveListener;
import com.coreos.jetcd.exception.EtcdExceptionFactory;
import com.coreos.jetcd.lease.LeaseRevokeResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class Session {
  public static int defaultSessionTTL = 60;
  
  private AtomicReference<Client> etcdclient;
  private long lease;
  private int ttl;
  private KeepAliveListener listener;
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private AtomicReference<Client> etcdclient;
    private long lease = 0L;
    private int ttl = defaultSessionTTL;
    
    public Builder setClient(Client client) {
      etcdclient = new  AtomicReference<Client>(client);
      return this;
    }
    
    public Builder setLease(long lease) {
      this.lease = lease;
      return this;
    }
    
    public Builder setTtl(int ttl) {
      this.ttl = ttl;
      return this;
    }
    
    public Session build() throws InterruptedException, ExecutionException {
      return new Session(lease, etcdclient, ttl);
    }
  }
  
  private Session(long lease, AtomicReference<Client> client, int ttl) 
      throws InterruptedException, ExecutionException {
    this.etcdclient = client;
    this.lease = lease;
    this.ttl = ttl;
    
    if (this.lease == 0) {
      this.lease = client.get().getLeaseClient().grant(this.ttl).get().getID();
    }
    listener = client.get().getLeaseClient().keepAlive(this.lease);
  }
  
  public void close() throws Exception {
    listener.close();
    CompletableFuture<LeaseRevokeResponse> future = etcdclient.get().getLeaseClient().revoke(lease);
    try {
      future.get(ttl,TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // if revoke takes longer than the ttl, lease is expired anyway
    } catch (InterruptedException | ExecutionException e) {
      throw EtcdExceptionFactory.newEtcdException("Exception when revoking lease " + lease);
    }
  }
  
  public Client getClient() {
    return etcdclient.get();
  }
  
  public long getLease() {
    return this.lease;
  }
}
