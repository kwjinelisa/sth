/*
 * Copyright © 2017 CMCC and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package com.coreos.jetcd.concurrency;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.annotations.Test;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.concurrency.Mutex;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchEvent.EventType;

public class LockTest extends AbstractConcurrencyTest{
  private static final String path2 = "root/dirC/dirD";
  
@Test
  public void testMixed() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    long revision = kvclient.get(PATH).get().getHeader().getRevision();
    Watcher watcher = newWatcherwithPfxRev(ByteSequence.fromString("root"), revision+1);
    
    Mutex[] mlist = new Mutex[6];
    Thread[] tlist = new Thread[6];

    mlist[0] = newUpdateMutexfromClient(client, "root/dirA");
    mlist[1] = newUpdateMutexfromClient(client, "root/dirC");
    mlist[2] = newInsertionMutexfromClient(client, "root/dirC");
    mlist[3] = newInsertionMutexfromClient(client, path2);
    mlist[4] = newDeleteMutexfromClient(client, path);
    mlist[5] = newDeleteMutexfromClient(client, "root");

    for(int i=0;i<6;i++) {
      tlist[i] = newLockThread(mlist[i], false);
      tlist[i].start();
      getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);  
    }
    
    tlist[0].join(1000);
    tlist[1].join(1000);
    test.assertTrue(tlist[2].isAlive());
    test.assertTrue(tlist[3].isAlive());
    tlist[4].join(1000);
    test.assertTrue(tlist[5].isAlive());
    
    mlist[1].unlock();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);
    tlist[2].join(1000);
    Thread.sleep(1000);
    test.assertTrue(tlist[3].isAlive());
    
    mlist[4].unlock();
    Thread.sleep(1000);
    test.assertTrue(tlist[5].isAlive());
    mlist[2].unlock();
    getEventsFromWatcherAndVerify(watcher, executor, 2, EventType.DELETE);
    tlist[3].join(1000);
    Thread.sleep(1000);
    test.assertTrue(tlist[5].isAlive());
    
    mlist[0].unlock();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);
    Thread.sleep(1000);
    test.assertTrue(tlist[5].isAlive());
    mlist[3].unlock();
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);
    tlist[5].join(1000);
    mlist[5].unlock();
  }

  @Test
  public void test2Ulocks() throws Exception{
    Mutex firstOwnerMutex = newUpdateMutexfromClient(client, path);
    /*launch the first thread to lock the firstowner*/
    Thread firstOwnerThread = newLockThread(firstOwnerMutex, false);
    firstOwnerThread.start();
    
    Mutex secondOwnerMutex =  newUpdateMutexfromClient(client, path);
    /*launch the second thread to lock the second owner, 
    which will be blocked until the unlocking of the first owner*/
    firstOwnerThread.join();
    Thread secondOwnerThread = newLockThread(secondOwnerMutex, false);
    secondOwnerThread.start();
    
    Thread.sleep(1000);
    test.assertTrue(secondOwnerThread.isAlive());
    test.assertTrue(!secondOwnerMutex.isOwner());
    
   /*verify the 2 PUT events generated by the 2 lock operations */
   Watcher watcher = newWatcherwithPfxRev(PATH, firstOwnerMutex.getRev());
   ExecutorService executor = Executors.newSingleThreadExecutor();
   List<WatchEvent> eventList = getEventsFromWatcherAndVerify(watcher, executor, 2, EventType.PUT);  

   
   /*unlock the first owner*/
   firstOwnerMutex.unlock();
   /*verify the DELETE event of the first owner*/
   getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);

   
   /*verify that the second lock has returned*/ 
   secondOwnerThread.join(1000);
   /*verify that the second lock is now the owner*/
   test.assertTrue(secondOwnerMutex.isOwner());
   /*unlock the second owner*/
   secondOwnerMutex.unlock();
   
   /*verify the DELETE event of the second owner*/
   eventList = getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);
   
   //test.assertEquals(secondOwnerSession.getLease(), 0l);
   //test.assertEquals(eventList.get(0).getKeyValue().getLease(), secondOwnerSession.getLease());
  }
  
  @Test
  /*testcase where the waiter prior to the acquirer expires before the current holder.*/
  public void test3Ulocks() throws Exception {
    Mutex firstOwnerMutex = newInsertionMutexfromClient(client, path);
    Mutex victimMutex = newInsertionMutexfromClient(client, path);
    Mutex secondOwnerMutex = newUpdateMutexfromClient(client, path);
    
    /*launch a new thread to lock the first owner*/
    Thread firstOwnerThread = newLockThread(firstOwnerMutex, false);
    firstOwnerThread.start();
    
    /*launch a new thread to lock the victim, which will be blocked*/
    firstOwnerThread.join(1000);
    Thread victimThread = newLockThread(victimMutex, false);
    victimThread.start(); 
    
    Watcher watcher =  newWatcherwithPfxRev(PATH, firstOwnerMutex.getRev()); 
    ExecutorService executor = Executors.newSingleThreadExecutor();
    List<WatchEvent> eventList = 
        getEventsFromWatcherAndVerify(watcher, executor, 2, EventType.PUT);

    
    /*launch a new thread to lock the secondowner, which will be blocked*/
    Thread secondOwnerThread = newLockThread(secondOwnerMutex, false);
    secondOwnerThread.start(); 
    getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.PUT);
    
    /*simulate losing the client that's next in line to acquire the lock*/
    victimMutex.getSession().close();
    /*verify the victim mutex is deleted*/
    eventList = getEventsFromWatcherAndVerify(watcher, executor, 1, EventType.DELETE);

    /*verify that the second owner has not yet acquired the lock*/
    Thread.sleep(1000);
    test.assertEquals(secondOwnerThread.isAlive(), true);
    test.assertTrue(!secondOwnerMutex.isOwner());
    
    /*unlock the first owner*/
    firstOwnerMutex.unlock();
    /*verify that the 2nd owner lock has returned*/
    secondOwnerThread.join(1000);
    /*verify that the second lock is now the owner*/
    test.assertTrue(secondOwnerMutex.isOwner());
    
    /*verify that the victim thread has terminated without acquiring the lock*/
    victimThread.join(1000);
    test.assertTrue(!victimMutex.isOwner());    
  }
}
