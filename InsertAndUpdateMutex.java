/*
 * Copyright © 2017 CMCC and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package com.coreos.jetcd.concurrency;

import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.data.Response.Header;
import com.coreos.jetcd.exception.EtcdException;
import com.coreos.jetcd.exception.EtcdExceptionFactory;
import com.coreos.jetcd.exception.WatchLostException;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.TxnResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class InsertAndUpdateMutex extends Mutex {

  protected InsertAndUpdateMutex(String prefix, Session session, String lockType) {
    super(prefix,  session,  lockType);
  }

  @Override
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

    if (ownerKV == null || ownerKV.getCreateRevision() == myrevision) {
      header = txnRes.getHeader();
      isOwner = true;
      return true;
    }
    
    /* wait for contenders prior to me to go away to acquire the lock*/
    header = waitForContendersToGo(myrevision - 1); 

    if (header == null) {
      this.unlock();
      throw EtcdExceptionFactory.newEtcdException("the lock got deleted during the lock() function "
          + "probably due to inactive session, renounce or retry");
    }

    isOwner = true;
    return true; 
    
  }

  private Header waitForContendersToGo(long maxCreateRev) 
      throws InterruptedException, ExecutionException, EtcdException {
    /*my contenders are insertion and deletion locks in the same directory
     * find the one contender whose create revision is:
     * 1.smaller than my revision
     * 2.highest among all contenders that fulfill condition 1 
     */
    final Cmp CMP = new Cmp(mykkey, Cmp.Op.EQUAL,CmpTarget.createRevision(0));    
    Op getUpdate = opWithLastMaxCreate(myprefix + "/update/", maxCreateRev);
    Op getInsert = opWithLastMaxCreate(myprefix + "/insert/", maxCreateRev);
    
    while (true) {
      KeyValue predecessor;
      long resRev;
      /*to check if my key already gets deleted
       * if it is the case, meaning the lock should be canceled*/
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
      /* to find if there are contenders prior to me 
       * if yes, find the one immediately in front of me to set a watch on
       * if no, then I should be the owner of the lock
       */
      if (resUpdate.getKvs().size() == 0) {
        /*if no one prior, the lock is ours*/
        if (resInsert.getKvs().size() == 0) {
          //no contender, return
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
      /*now we have found the contender to set a watch on*/
      try {
        setWatch(predecessor.getKey(), resRev);
      } catch (WatchLostException e) {
        /*if the watch got lost, no big deal, just continue with the loop and try again*/
        System.out.println(e.getMessage());
      }
    }
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
  
  
  private Op[] getAllContenders() {
    Op[] deleteOps = opsForDeletes();
    Op updateOp = opWithFirstCreate(myprefix + "/update/");
    Op insertOp = opWithFirstCreate(myprefix + "/insert/");
    return concat(deleteOps, updateOp, insertOp);
  }
  
  private Op[] opsForDeletes() {
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
}
