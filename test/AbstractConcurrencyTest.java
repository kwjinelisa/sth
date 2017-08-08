/*
 * Copyright © 2017 CMCC and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package com.coreos.jetcd.concurrency;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.asserts.Assertion;
import com.coreos.jetcd.Client;
import com.coreos.jetcd.ClientBuilder;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.internal.impl.TestConstants;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchEvent.EventType;
import com.coreos.jetcd.watch.WatchResponse;

public abstract class AbstractConcurrencyTest {
  protected static final String path = "root/dirA/dirB";
  protected static final ByteSequence PATH = ByteSequence.fromString("root/dirA/dirB");
  protected Assertion test;
  protected Client client;
  
  @BeforeTest
  public void setUp() throws Exception {
    test = new Assertion();
    client = ClientBuilder.newBuilder().setEndpoints(TestConstants.endpoints).build();
  }
  
  @AfterTest
  public void tearDown() {
    this.client.close();
  }

  protected Watcher newWatcherwithPfxRev(ByteSequence prefix, long revision) {
    return client.getWatchClient().watch(prefix, 
        WatchOption.newBuilder().withPrefix(prefix).withRevision(revision).build());
  }
  
  protected void getEventsFromWatcherAndVerify(Watcher watcher, 
      ExecutorService executor, int numEvents, EventType type) throws Exception {
    List<WatchEvent> eventList = getEventsFromWatcher(watcher, executor, numEvents);
    test.assertEquals(eventList.size(), numEvents);
    for (WatchEvent event:eventList) {
      test.assertEquals(event.getEventType(),type);
    }
  }
  
  private List<WatchEvent> getEventsFromWatcher(Watcher watcher, 
      ExecutorService executor, int numEvents) throws Exception {
    Future<List<WatchEvent>> future = executor.submit(() -> {
      int count = 0;
      List<WatchEvent> events = new ArrayList();
      while (count < numEvents) {
        WatchResponse wchRes = watcher.listen();
        count += wchRes.getEvents().size();
        events.addAll(wchRes.getEvents());
      }
      return events;
    });
    return future.get(1, TimeUnit.SECONDS);
  }
  
  protected Mutex newUMutexfromClient(Client client, String prefix) throws InterruptedException, ExecutionException {
    return newMutexfromClient(client, prefix, UMutex::new);
  }
  
  protected Mutex newMutexfromClient(Client client, String prefix, MutexFactory factory)
      throws InterruptedException, ExecutionException{ 
    Session session = Session.newBuilder().setClient(client).build();
    return factory.apply(prefix, session);
  }
  
  protected interface MutexFactory {
    Mutex apply(String str, Session session);
  }
}
