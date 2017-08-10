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
import com.coreos.jetcd.exception.WatchLostException;
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
  
  protected UMutex(String prefix, Session session, String lockType) {
    super(prefix,  session,  lockType);
  }

  public boolean lock() throws ExecutionException, InterruptedException, EtcdException {
    
    final Cmp CMP = new Cmp(ByteSequence.fromString(mykey), 
            Cmp.Op.EQUAL,CmpTarget.createRevision(0));    
    final Op put = Op.put(ByteSequence.fromString(mykey), ByteSequence.fromString(""), 
            PutOption.newBuilder().withLeaseId(mysession.getLease()).build());
    final Op get = Op.get(ByteSequence.fromString(mykey), GetOption.DEFAULT);
    
    final Op[] getOwner = getOpsforFindingOwner();
    final Op[] thenOps = concat(put, getOwner);
    final Op[] elseOps = concat(get, getOwner);

    CompletableFuture<TxnResponse> txnFuture = myKvclient.txn()
            .If(CMP).Then(thenOps).Else(elseOps).commit();
    
    TxnResponse txnRes = txnFuture.get();
    myrevision = txnRes.getHeader().getRevision();
    int ownerIndex = 0;
    
    if (!txnRes.isSucceeded()) {
      ownerIndex = 1;
      myrevision = txnRes.getGetResponses().get(0).getKvs().get(0).getCreateRevision();
    }
    
    KeyValue ownerKV = getOwnerKV(txnRes.getGetResponses(), ownerIndex);
    /*if a Delete lock encompassing our update lock was found, 
     * cancel our lock by returning false */
    if (ownerKV != null && ownerKV.getKey().toString().contains("/Delete/")) {
      unlock();
      return false;
    }
    
    if (ownerKV == null || ownerKV.getCreateRevision() == myrevision) {
      header = txnRes.getHeader();
      isOwner = true;
      return true;
    }
    
    header = waitForPredecessor(myrevision - 1, myclient); 
    if (header == null) {
      this.unlock();
      throw EtcdExceptionFactory.newEtcdException("the lock got deleted during the lock() function "
          + "probably due to inactive session, renounce or retry");
    }

    isOwner = true;
    return true; 
  }

  public void unlock() throws InterruptedException, ExecutionException {
    mysession.closeListener();
    Client client = mysession.getClient();
    KV kvclient = client.getKVClient();
    kvclient.delete(ByteSequence.fromString(mykey)).get();
    mykey = null;
    myrevision = -1;
    isOwner = false;
  }
  
  private void watchPredecessor(ByteSequence key, Client client, long revision) 
               throws CompactedException, EtcdException {
    
    Watcher watcher = client.getWatchClient().watch(key, 
                      WatchOption.newBuilder().withRevision(revision).withNoPut(true).build());
    try {
      WatchResponse watchRes = watcher.listen();
      if (!watchRes.getEvents().get(0).getEventType().equals(EventType.DELETE)) {
        throw EtcdExceptionFactory.newWatchLostException(key.toStringUtf8());
      }
    } finally {
      watcher.close();
    }
  }
  
  private Header waitForPredecessor(long maxCreateRev, Client client) 
      throws InterruptedException, ExecutionException, EtcdException {

    ByteSequence pfxUpdate = ByteSequence.fromString(myprefix + "/update/");
    ByteSequence pfxInsert = ByteSequence.fromString(myprefix + "/insert/");
    GetOption optionU = withLastMaxCreate(pfxUpdate, maxCreateRev);
    GetOption optionI = withLastMaxCreate(pfxInsert, maxCreateRev);
    
    final Cmp CMP = new Cmp(ByteSequence.fromString(mykey), 
        Cmp.Op.EQUAL,CmpTarget.createRevision(0));    
    Op getUpdate = Op.get(pfxUpdate, optionU);
    Op getInsert = Op.get(pfxInsert, optionI);
    
    while (true) {
      KeyValue predecessor;
      long resRev;
      
      CompletableFuture<TxnResponse> txnFuture = myKvclient.txn()
          .If(CMP).Then().Else(getUpdate, getInsert).commit();
      TxnResponse txnRes = txnFuture.get();
      if (txnRes.isSucceeded()) {
        /*the key representing this mutex has been deleted, probably due 
         * to inactive session, thus we should abandon the locking operation*/
        return null;
      }
      GetResponse resUpdate = txnRes.getGetResponses().get(0);
      GetResponse resInsert = txnRes.getGetResponses().get(1);
      
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
      try {
        watchPredecessor(predecessor.getKey(), client, resRev);
      } catch (WatchLostException e) {
        /*if the watch got lost, no big deal, just continue with the loop and try again*/
        System.out.println(e.getMessage());
      }
    }
  }
  
  private Op[] getOpsforFindingOwner() {
    Op[] getLockDelete = searchForDeleteLock();
    Op getOwnerUpdate = opWithFirstCreate(myprefix + "/update/");
    Op getOwnerInsert = opWithFirstCreate(myprefix + "/insert/");
    return concat(getLockDelete, getOwnerUpdate, getOwnerInsert);
  }
  
  private Op[] searchForDeleteLock() {
    String[] parts = myprefix.split("/");
    int len = parts.length;
    Op[] getLockDelete = new Op[len + 1];
    
    String start = "";
    getLockDelete[0] = opWithFirstCreate(start + "delete/");

    for (int i = 0;i < len;i++) {
      start = start + parts[i] + "/";
      String prefix = start + "delete/";
      getLockDelete[i + 1] = opWithFirstCreate(prefix);
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
        if (candidateCreate > 0 && candidateCreate <= myrevision) {
          
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
}
