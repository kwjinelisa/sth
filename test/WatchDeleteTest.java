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
import com.coreos.jetcd.watch.WatchEvent.EventType;


public class WatchDeleteTest extends AbstractConcurrencyTest {

  private static final String key1 = path + "/update/10000";
  private static final ByteSequence KEY1 = ByteSequence.fromString(key1);
  private static final String key2 = path + "/update/20000";
  private static final ByteSequence KEY2 = ByteSequence.fromString(key2);
  
  @Test
  public void testPredecessorChange() throws Exception{
    /*put the first key*/
    long revision = client.getKVClient().put(KEY1, ByteSequence.fromString(""))
        .get().getHeader().getRevision();
    /*verify the put operation from watcher*/
    Watcher watcher =  newWatcherwithPfxRev(PATH, revision);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);
    
    /*put the second key*/
    client.getKVClient().put(KEY2, ByteSequence.fromString("")).get();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);
    
    Mutex owner = newUMutexfromClient(client, path);
    Thread ownerlockThread = newLockThread(owner, false);
    ownerlockThread.start();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);
    
    client.getKVClient().delete(KEY2).get();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);
    
    test.assertTrue(ownerlockThread.isAlive());
    
    client.getKVClient().delete(KEY1).get();
    ownerlockThread.join(1000);
    test.assertTrue(owner.isOwner());
  }
}
