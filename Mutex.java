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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public abstract class Mutex {
  protected String myprefix;
  protected String mykey;
  protected ByteSequence mykkey;
  protected long myrevision;
  protected Session mysession;
  protected Client myclient;
  protected Header header;
  protected boolean isOwner;
  protected KV myKvclient;
    
  protected Mutex(String prefix, Session session, String lockType) {
    myprefix = prefix;
    mysession = session;
    mykey = myprefix + "/" + lockType + "/" + mysession.getLease();
    mykkey = ByteSequence.fromString(mykey);
    myclient = mysession.getClient();
    myKvclient = myclient.getKVClient();
  }

  public static Mutex newUpdateMutex(String prefix, Session session) {
    return new InsertAndUpdateMutex(prefix, session, "update");
  }
  
  public static Mutex newInsertionMutex(String prefix, Session session) {
    return new InsertAndUpdateMutex(prefix, session, "insert");
  }
  
  public static Mutex newDeletMutex(String prefix, Session session) {
    return new DeleteMutex(prefix, session, "delete");
  } 
  
  public boolean lock() throws Exception {
    final Cmp CMP = new Cmp(mykkey, Cmp.Op.EQUAL,CmpTarget.createRevision(0));     
    final Op put = Op.put(mykkey, ByteSequence.fromString(""), 
        PutOption.newBuilder().withLeaseId(mysession.getLease()).build());
    final Op get = Op.get(mykkey, GetOption.DEFAULT);
    
    final Op[] getOwner = getAllContenders();
    final Op[] thenOps = concat(put, getOwner);
    final Op[] elseOps = concat(get, getOwner);
    
    CompletableFuture<TxnResponse> txnFuture = myKvclient.txn()
        .If(CMP).Then(thenOps).Else(elseOps).commit();
    TxnResponse txnRes = txnFuture.get();
    myrevision = txnRes.getHeader().getRevision();
    int contenderStartingIndex = 0;
    
    if (!txnRes.isSucceeded()) {
      contenderStartingIndex = 1;
      myrevision = txnRes.getGetResponses().get(0).getKvs().get(0).getCreateRevision();
    }

    KeyValue ownerKV = getOwnerKV(txnRes.getGetResponses(), contenderStartingIndex);
    /*if a Delete lock encompassing our update lock was found, 
     * cancel our lock by returning false */
    if (ownerKV != null && ownerKV.getKey().toString().contains("/delete/")) {
      unlock();
      return false;
    }
    
    /* wait for contenders prior to me to go away to acquire the lock*/
    header = waitForContendersToGo(myrevision - 1); 

    if (header == null) {
      String temp = mykey;
      this.unlock();
      throw EtcdExceptionFactory.newEtcdException("the key " + temp 
          + " got deleted during the lock() function "
          + "probably due to inactive session, renounce or retry");
    }

    isOwner = true;
    return true; 
  }
  
  public void unlock() throws Exception {
    mysession.closeListener();
    myKvclient.delete(mykkey).get();
    mykey = null;
    mykkey = null;
    myrevision = -1;
    isOwner = false;
  }
  
  public boolean isOwner() {
    return isOwner;
  }
  
  protected abstract Op[] getOtherContendersOp(); 
  
  /*return a collection of Ops for getting the most recently created locks
   *  respectively in each path, each Op represents a path to inspect
   *  the choice of paths to search depends on the lock type
   *  each Op will generate a GetResponse in the Txn response*/
  protected abstract Op[] getContendersImmediatelyBefore(long maxCreateRev);

  
  protected KeyValue getOwnerKV(List<GetResponse> getRes, int startIndex) {
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
          if (i < getRes.size() - 1) {
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

  
  /*among all the get responses on the list, 
   * locate the one that is created the most recently
   * (i.e lock with the biggest create revision)
   */
  protected GetResponse findContenderImmediatelyBefore(List<GetResponse> responses) {
    GetResponse contenderToReturn = null;
    long maxCreateRev = -1;
    
    for (GetResponse r:responses) {
      if (r.getKvs().size() == 0) {
        continue;
      }
      long rev = r.getKvs().get(0).getCreateRevision();
      if (maxCreateRev == -1 || rev > maxCreateRev) {
        contenderToReturn = r;
        maxCreateRev = rev;
      } 
    }
    return contenderToReturn;
  }
  
  
  protected Header waitForContendersToGo(long maxCreateRev) 
      throws InterruptedException, ExecutionException, EtcdException {
    /*my contenders are insertion and deletion locks in the same directory
     * find the one contender whose create revision is:
     * 1.smaller than my revision
     * 2.highest among all contenders that fulfill condition 1 
     */
    final Cmp CMP = new Cmp(mykkey, Cmp.Op.EQUAL,CmpTarget.createRevision(0));    
    Op[] getContenders = getContendersImmediatelyBefore(maxCreateRev);
    
    while (true) {
      KeyValue targetToWatch;
      long revisionToWatchFrom;
      /*to check if my key already gets deleted
       * if it is the case, meaning the lock should be canceled*/
      CompletableFuture<TxnResponse> txnFuture = myKvclient.txn()
          .If(CMP).Then().Else(getContenders).commit();
      TxnResponse txnRes = txnFuture.get();
      if (txnRes.isSucceeded()) {
        /*the key representing this mutex has been deleted, probably due 
         * to inactive session, thus we should abandon the locking operation*/
        return null;
      }
           
      /* to find if there are contenders prior to me 
       * if yes, find the one immediately in front of me to set a watch on
       * if no, then I should be the owner of the lock
       */
      GetResponse target = findContenderImmediatelyBefore(txnRes.getGetResponses());

      if (target == null) {
        /*if no one prior, the lock is ours*/
        return txnRes.getHeader();
      } else {
        targetToWatch = target.getKvs().get(0);
        revisionToWatchFrom = target.getHeader().getRevision();
      }
      
      /*now we have found the contender to set a watch on*/
      try {
        setWatch(targetToWatch.getKey(), revisionToWatchFrom);
      } catch (WatchLostException e) {
        /*if the watch got lost, no big deal, just continue with the loop and try again*/
        System.out.println(e.getMessage());
      }
    }
  }

  protected void setWatch(ByteSequence key, long revision) 
      throws CompactedException, EtcdException {
    /*wait for the contender to go away*/
    Watcher watcher = myclient.getWatchClient().watch(key, 
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
  
  protected Op[] getAllContenders() {
    /*a delete lock in the parent path is a contender
     *  no matter the lock type*/
    Op[] getDeleteContenders = opsForDeletes();
    //other contenders depend on the lock type
    Op[] others =  getOtherContendersOp();
    return concat(getDeleteContenders, others);
  }
  
  protected Op[] opsForDeletes() {
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
  
  protected Op opWithFirstCreate(String prefix) {
    ByteSequence pprefix = ByteSequence.fromString(prefix);
    return Op.get(pprefix, withFirstCreate(pprefix));
  }
  
  protected GetOption withFirstCreate(ByteSequence prefix) {
    return GetOption.newBuilder().withLimit(1)
        .withSortOrder(SortOrder.ASCEND)
        .withSortField(SortTarget.CREATE)
        .withPrefix(prefix).build();
  }
  
  protected Op opWithLastMaxCreate(String prefix, long maxCreateRev) {
    ByteSequence pprefix = ByteSequence.fromString(prefix); 
    return Op.get(pprefix, withLastMaxCreate(pprefix, maxCreateRev));
  }
   
  protected GetOption withLastMaxCreate(ByteSequence prefix, long maxcreateRev) {
    return GetOption.newBuilder().withLimit(1)
        .withSortOrder(SortOrder.DESCEND)
        .withSortField(SortTarget.CREATE)
        .withPrefix(prefix)
        .withMaxCreateRevision(maxcreateRev)
        .build();
  }
  
  protected Op[] concat(Op[] a, Op... b) {
    int alen = a.length;
    int blen = b.length;
    Op[] c = new Op[alen + blen];
    System.arraycopy(a, 0, c, 0, alen);
    System.arraycopy(b, 0, c, alen, blen);
    return c;
  }
  
  protected Op[] concat(Op a, Op[] b) {
    return concat(new Op[]{a}, b);
  }
  
  public long getRev() {
    return myrevision;
  }

  public String getKey() {
    return mykey;
  }
  
  public Session getSession() {
    return mysession;
  }
  
}
