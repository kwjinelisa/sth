package com.coreos.jetcd.concurrency;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.data.Response.Header;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.TxnResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.GetOption.SortOrder;
import com.coreos.jetcd.options.GetOption.SortTarget;
import com.coreos.jetcd.options.PutOption;
import java.lang.InterruptedException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class UMutex extends Mutex {
  public UMutex(String prefix, Session session) {
    super(prefix, session);
  }

  public boolean lock() throws ExecutionException, InterruptedException {
    Client client = this.session.getClient();
    KV kvclient = client.getKVClient();
    key = pfx + "/update/" + session.getLease();
    
    final Cmp CMP = new Cmp(ByteSequence.fromString(key), 
            Cmp.Op.EQUAL,CmpTarget.createRevision(0));    
    final Op put = Op.put(ByteSequence.fromString(key), ByteSequence.fromString(""), 
            PutOption.newBuilder().withLeaseId(session.getLease()).build());
    final Op get = Op.get(ByteSequence.fromString(key), GetOption.DEFAULT);
    
    final Op[] getOwner = getOwnerOps();
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
      return false;
    }
    
    if (ownerKV == null || ownerKV.getCreateRevision() == revision) {
      header = txnRes.getHeader();
      return true;
    }
    
    
    return true;
    
  }

  public void unlock() {

  }
  
  public boolean isOwner() {
    return true;
  }
  
  private Op[] getOwnerOps() {
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
    getLockDelete[0] = getOpWithFirstCreate(start + "/delete/");

    for (int i = 0;i < len;i++) {
      start = start + parts[i];
      String prefix = start + "/delete/";
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
        .withPrefix(ByteSequence.fromString(pfx)).build();
  }
  
  
}
