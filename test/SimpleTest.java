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

public class SimpleTest extends AbstractConcurrencyTest {
  private static final String subpath1 = path + "/dirC";
  private static final String subpath2 = path + "/dirD";

  @Test
  public void deleteBlocksDelete() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newDeleteMutexfromClient(client, path);
    Mutex second = newDeleteMutexfromClient(client, subpath1);
    firstBlocksSecond(first, second, executor, null);
  }
  
  @Test
  public void deleteBlocksDelete2() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newDeleteMutexfromClient(client, subpath1);
    Mutex second = newDeleteMutexfromClient(client, path);
    firstBlocksSecond(first, second, executor, null);
  }
  
  @Test
  public void deleteNotBlockDelete() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newDeleteMutexfromClient(client, subpath1);
    Mutex second = newDeleteMutexfromClient(client, subpath2);
    firstNotBlockSecond(first, second, executor, null);
  }
  
  @Test
  public void deleteBlocksUpdate() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newDeleteMutexfromClient(client, path);
    Mutex second = newUpdateMutexfromClient(client, subpath1);
    firstBlocksSecond(first, second, executor, null);
  }
  
  @Test
  public void deleteNotBlockUpdate() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newDeleteMutexfromClient(client, subpath1);
    Mutex second = newUpdateMutexfromClient(client, subpath2);
    firstNotBlockSecond(first, second, executor, null);
  }
  
  @Test
  public void deleteNotBlockUpdate2() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newDeleteMutexfromClient(client, subpath1);
    Mutex second = newUpdateMutexfromClient(client,path);
    firstNotBlockSecond(first, second, executor, null);
  }
  
  @Test
  public void deleteBlocksInsert() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newDeleteMutexfromClient(client, subpath1);
    Mutex second = newInsertionMutexfromClient(client, path);
    firstBlocksSecond(first, second, executor, null);
  }
  
  @Test
  public void deleteBlocksInsert2() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newDeleteMutexfromClient(client, path);
    Mutex second = newInsertionMutexfromClient(client, subpath2);
    firstBlocksSecond(first, second, executor, null);
  }
  
  @Test
  public void updateBlocksUpdate() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newUpdateMutexfromClient(client, path);
    Mutex second = newUpdateMutexfromClient(client, path);
    firstBlocksSecond(first, second, executor, null);
  }
  
  @Test
  public void updateBlocksInsert() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newUpdateMutexfromClient(client, subpath1);
    Mutex second = newInsertionMutexfromClient(client, path);
    firstBlocksSecond(first, second, executor, null);
  }
  
  @Test
  public void updateNotBlockUpdate() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newUpdateMutexfromClient(client, path);
    Mutex second = newUpdateMutexfromClient(client, subpath1);
    firstNotBlockSecond(first, second, executor, null);
  }
  
  @Test
  public void updateNotBlockInsert() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Mutex first = newUpdateMutexfromClient(client, path);
    Mutex second = newInsertionMutexfromClient(client, subpath1);
    firstNotBlockSecond(first, second, executor, null);
  }
}
