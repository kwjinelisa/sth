/*
 * Copyright © 2017 CMCC and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package com.coreos.jetcd.concurrency;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.testng.annotations.Test;

import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.PutResponse;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchEvent.EventType;


public class WatchDeleteTest extends AbstractConcurrencyTest {

  private static final String key1 = path + "/update/10000";
  private static final ByteSequence KEY1 = ByteSequence.fromString(key1);
  private static final String key2 = path + "/update/20000";
  private static final ByteSequence KEY2 = ByteSequence.fromString(key2);
  private int count = 0;
  
  @Test
  /*the simplest scenario where the mutex waits for 
   * the current owner's deletion in order to become the owner*/
  public void testOneInFront() throws Exception{
    long lease = client.getLeaseClient().grant(10).get().getID();
    /*simulate a owner in place by sending put*/
    long revision = putWithLease(KEY1, lease).getHeader().getRevision();
    
    Watcher watcher =  newWatcherwithPfxRev(PATH, revision);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    /*verify the put operation from watcher*/
    List<WatchEvent> list = getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);
    test.assertEquals(list.get(0).getKeyValue().getKey(), KEY1);
    
    /*try to lock the mutex*/
    Mutex owner = newUpdateMutexfromClient(client, path);
    Thread ownerlockThread = newLockThread(owner, false);
    ownerlockThread.start();
    list = getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);
    test.assertEquals(list.get(0).getKeyValue().getKey(), ByteSequence.fromString(owner.getKey()));
    /*veirfy that the lock() has not returned*/
    Thread.sleep(1000);
    test.assertTrue(!owner.isOwner());
    test.assertTrue(ownerlockThread.isAlive());
    
    //delete the current owner key
    client.getKVClient().delete(KEY1).get();
    
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);

    /*verify the mutex has now become the owner*/
    ownerlockThread.join(1000);
    test.assertTrue(owner.isOwner());
    
    owner.unlock();
  }
  
  @Test
  public void testPredecessorChange() throws Exception{
    /*put the first key*/
    long lease = client.getLeaseClient().grant(10).get().getID();
    long revision = putWithLease(KEY1, lease).getHeader().getRevision();
    Watcher watcher =  newWatcherwithPfxRev(PATH, revision);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    
    /*verify the put operation from watcher*/
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);
    
    /*put the second key and verify*/
    putWithLease(KEY2,lease);
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);
    /*try to lock*/
    Mutex owner = newUpdateMutexfromClient(client, path);
    Thread ownerlockThread = newLockThread(owner, false);
    ownerlockThread.start();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);
    /*delete the second key, after which the mutex should not acquire the lock*/
    client.getKVClient().delete(KEY2).get();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);
    Thread.sleep(1000);
    test.assertTrue(ownerlockThread.isAlive());
    /*delete the first key, after which the mutex should acquire the lock*/
    client.getKVClient().delete(KEY1).get();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);
    ownerlockThread.join(1000);
    test.assertTrue(owner.isOwner());
  }
  
  @Test
  public void testNInFront() throws Exception {
    long lease = client.getLeaseClient().grant(10).get().getID();
    final int N = 5;
    ExecutorService executor = Executors.newSingleThreadExecutor();
    /*put N keys in the directory to simulate N lock requests in front*/
    Watcher watcher = putNKeysInRollwithLease(N, lease, executor, null);
    
    /*try to lock*/
    Mutex owner = newUpdateMutexfromClient(client, path);
    Thread ownerlockThread = newLockThread(owner, false);
    ownerlockThread.start();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);
    /*delete all keys except the first one in reverse order, i.e
     *  N,N-1,N-2, ... until 2 
     *  so that the mutex must change a predecessor to watch for each time*/
    for (int i=0;i<N-1;i++) {
      String keyToDelete = path + "/update/" + (count-i);
      ByteSequence keyToDelete_ = ByteSequence.fromString(keyToDelete);
      kvclient.delete(keyToDelete_).get();
      getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);
    }
    /*at this point the lock() should not have already returned*/
    Thread.sleep(1000);
    test.assertTrue(ownerlockThread.isAlive());
    
    kvclient.delete(ByteSequence.fromString(path + "/update/" + (count-N+1))).get();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);
    Thread.sleep(1000);
    test.assertTrue(!ownerlockThread.isAlive());
    test.assertTrue(owner.isOwner());
    owner.unlock();
  }
   
  private PutResponse putWithLease(ByteSequence key, long lease) throws InterruptedException, ExecutionException {
   return kvclient.put(key, ByteSequence.fromString("")
             ,PutOption.newBuilder().withLeaseId(lease).build()).get();
  }
  
  private Watcher putNKeysInRollwithLease(int n, long lease, 
      ExecutorService executor, Watcher watcher) throws Exception {
    Watcher thiswatcher = watcher;
    int start = 0;
    if (thiswatcher == null) {
      String keyToPut =  path + "/update/" + (++count);
      ByteSequence keyToPut_ = ByteSequence.fromString(keyToPut);
      long revision = putWithLease(keyToPut_, lease).getHeader().getRevision();
      thiswatcher =  newWatcherwithPfxRev(PATH, revision);
      /*verify the put operation from watcher*/
      getEventsFromWatcherAndVerify(thiswatcher, executor, 1, EventType.PUT);
      start = 1;
    }
    
    for (int i=start;i<n;i++) {
      String keyToPut =  path + "/update/" + (++count);
      ByteSequence keyToPut_ = ByteSequence.fromString(keyToPut);
      putWithLease(keyToPut_, lease).getHeader().getRevision();
      /*verify the put operation from watcher*/
      getEventsFromWatcherAndVerify(thiswatcher, executor, 1, EventType.PUT);
    }
    return thiswatcher;

  }
}


