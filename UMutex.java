package com.coreos.jetcd.concurrency;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.data.Response.Header;
import com.coreos.jetcd.exception.CompactedException;
import com.coreos.jetcd.exception.EtcdException;
import com.coreos.jetcd.exception.EtcdExceptionFactory;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.TxnResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.GetOption.SortOrder;
import com.coreos.jetcd.options.GetOption.SortTarget;
import com.coreos.jetcd.options.PutOption;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchEvent.EventType;
import com.coreos.jetcd.watch.WatchResponse;

import java.lang.InterruptedException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class UMutex extends Mutex {
  public UMutex(String prefix, Session session) {
    super(prefix, session);
  }

  public boolean lock() throws ExecutionException, InterruptedException, EtcdException {
    Client client = this.session.getClient();
    KV kvclient = client.getKVClient();
    key = pfx + "/update/" + session.getLease();
    //key = pfx + "/update/" + 3000;
    
    final Cmp CMP = new Cmp(ByteSequence.fromString(key), 
            Cmp.Op.EQUAL,CmpTarget.createRevision(0));    
    final Op put = Op.put(ByteSequence.fromString(key), ByteSequence.fromString(""), 
            PutOption.newBuilder().withLeaseId(session.getLease()).build());
    final Op get = Op.get(ByteSequence.fromString(key), GetOption.DEFAULT);
    
    final Op[] getOwner = getOpsforFindingOwner();
    final Op[] thenOps = concat(put, getOwner);
    final Op[] elseOps = concat(get, getOwner);

    CompletableFuture<TxnResponse> txnFuture = kvclient.txn()
            .If(CMP).Then(thenOps).Else(elseOps).commit();
    
    TxnResponse txnRes = txnFuture.get();
    revision = txnRes.getHeader().getRevision();
    int ownerIndex = 0;
    
    if (!txnRes.isSucceeded()) {
      ownerIndex = 1;
      revision = txnRes.getGetResponses().get(0).getKvs().get(0).getCreateRevision();
    }
    
    KeyValue ownerKV = getOwnerKV(txnRes.getGetResponses(), ownerIndex);
    /*if a Delete lock encompassing our update lock was found, 
     * cancel our lock by returning false */
    if (ownerKV != null && ownerKV.getKey().toString().contains("/Delete/")) {
      unlock();
      return false;
    }
    
    if (ownerKV == null || ownerKV.getCreateRevision() == revision) {
      header = txnRes.getHeader();
      isOwner = true;
      return true;
    }
    
    header = waitForPredecessor(revision - 1, client); 
    
    if (kvclient.get(ByteSequence.fromString(key)).get().getKvs().size() == 0) {
      this.unlock();
      throw EtcdExceptionFactory.newEtcdException("the lock got deleted during the lock() function "
          + "probably due to inactive session, renounce or retry");
    }
    isOwner = true;
    return true; 
  }

  public void unlock() throws InterruptedException, ExecutionException {
    session.closeListener();
    Client client = session.getClient();
    KV kvclient = client.getKVClient();
    kvclient.delete(ByteSequence.fromString(key)).get();
    key = null;
    revision = -1;
    isOwner = false;
  }
  
  public boolean isOwner() {
    return isOwner;
  }
  
  
  private void watchPredecessor(String key, Client client, long revision) 
               throws CompactedException, EtcdException {
    
    Watcher watcher = client.getWatchClient().watch(ByteSequence.fromString(key), 
                      WatchOption.newBuilder().withRevision(revision).withNoPut(true).build());
    try {
      
      WatchResponse watchRes = watcher.listen();
      if (watchRes.getEvents().get(0).getEventType().equals(EventType.DELETE)) {
        return;
      }
      throw EtcdExceptionFactory.newEtcdException("lost watcher waiting for delete");
      
    } finally {
      watcher.close();
    }
  }
  
  private Header waitForPredecessor(long maxCreateRev, Client client) 
      throws InterruptedException, ExecutionException, EtcdException {
    String pfxUpdate = pfx + "/update/";
    String pfxInsert = pfx + "/insert/";
    GetOption optionU = withLastMaxCreate(pfxUpdate, maxCreateRev);
    GetOption optionI = withLastMaxCreate(pfxInsert, maxCreateRev);
    
    while (true) {
      KeyValue predecessor;
      long resRev;;
      CompletableFuture<GetResponse> updateFuture = client.getKVClient().get(
          ByteSequence.fromString(pfxUpdate), optionU);
      
      CompletableFuture<GetResponse> insertFuture = client.getKVClient().get(
          ByteSequence.fromString(pfxInsert), optionI);
      GetResponse resUpdate = updateFuture.get();
      GetResponse resInsert = insertFuture.get();
      
      if (resUpdate.getKvs().size() == 0) {
        /*if no one prior, the lock is ours*/
        if (resInsert.getKvs().size() == 0) {
          return resInsert.getHeader();
        }
        predecessor = resInsert.getKvs().get(0);
        resRev = resInsert.getHeader().getRevision();
      } else {
        long updateCreateRev = resUpdate.getKvs().get(0).getCreateRevision();
        if (resInsert.getKvs().size() == 0 
            || resInsert.getKvs().get(0).getCreateRevision() < updateCreateRev) {
          predecessor = resUpdate.getKvs().get(0);
          resRev = resUpdate.getHeader().getRevision();
        } else {
          predecessor = resInsert.getKvs().get(0);
          resRev = resInsert.getHeader().getRevision();

        }
      }
      /*now we have found the predecessor to set a watch on*/
      watchPredecessor(predecessor.getKey().toString(), client, resRev);      
    }
  }
  
  private Op[] getOpsforFindingOwner() {
    Op[] getLockDelete = searchForDeleteLock();
    Op getOwnerUpdate = getOpWithFirstCreate(pfx + "/update/");
    Op getOwnerInsert = getOpWithFirstCreate(pfx + "/insert/");
    return concat(getLockDelete, getOwnerUpdate, getOwnerInsert);
  }
  
  private Op[] searchForDeleteLock() {
    String[] parts = pfx.split("/");
    int len = parts.length;
    Op[] getLockDelete = new Op[len + 1];
    
    String start = "";
    getLockDelete[0] = getOpWithFirstCreate(start + "delete/");

    for (int i = 0;i < len;i++) {
      start = start + parts[i] + "/";
      String prefix = start + "delete/";
      getLockDelete[i + 1] = getOpWithFirstCreate(prefix);
    }
    return getLockDelete;
  }
  
  private KeyValue getOwnerKV(List<GetResponse> getRes, int startIndex) {
    KeyValue ownerKV = null;
    long ownerCreateRevision = 0;
    for (int i = startIndex; i < getRes.size(); i++) {
      List<KeyValue> candidateKVs = getRes.get(i).getKvs();
      if (candidateKVs.size() != 0) {
        KeyValue candidateKV = candidateKVs.get(0);
        long candidateCreate = candidateKV.getCreateRevision();
        if (candidateCreate > 0 && candidateCreate <= revision) {
          
          /* if we find a delete lock created prior to our update lock and 
           * encompassing our path, no need to search further,
           * our update lock should be canceled*/
          if (i < getRes.size() - 2) {
            return candidateKV;
          }
          if (ownerCreateRevision == 0 || candidateCreate < ownerCreateRevision) {
            ownerCreateRevision = candidateCreate;
            ownerKV = candidateKV;
          }
        }
      }
    }
    return ownerKV;
  }
  
  private Op getOpWithFirstCreate(String prefix) {
    return Op.get(ByteSequence.fromString(prefix), 
              withFirstCreate(prefix));
  }
  

  private GetOption withFirstCreate(String prefix) {
    return GetOption.newBuilder().withLimit(1)
        .withSortOrder(SortOrder.ASCEND)
        .withSortField(SortTarget.CREATE)
        .withPrefix(ByteSequence.fromString(prefix)).build();
  }
  
  private GetOption withLastMaxCreate(String prefix, long maxcreateRev) {
    return GetOption.newBuilder().withLimit(1)
        .withSortOrder(SortOrder.DESCEND)
        .withSortField(SortTarget.CREATE)
        .withPrefix(ByteSequence.fromString(prefix))
        .withMaxCreateRevision(maxcreateRev)
        .build();
  }
}
