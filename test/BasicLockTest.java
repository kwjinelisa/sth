/*
 * Copyright © 2017 CMCC and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package com.coreos.jetcd.concurrency;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.testng.annotations.Test;

import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.watch.WatchEvent.EventType;

public class BasicLockTest extends AbstractConcurrencyTest {

  @Test
  public void testLockUnlock() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();

    Mutex owner = newUpdateMutexfromClient(client, path);
    Thread lockThread = newLockThread(owner, false);
    lockThread.start();
    lockThread.join();
    
    Watcher watcher =  newWatcherwithPfxRev(PATH, owner.getRev());
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);
    
    Thread.sleep(1000);
    
    owner.unlock();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);
  }
  
}
