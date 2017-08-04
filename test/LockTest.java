/*
 * Copyright © 2017 CMCC and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package com.cores.jetcd.concurrency;

import org.testng.asserts.Assertion;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.ClientBuilder;
import com.coreos.jetcd.concurrency.Mutex;
import com.coreos.jetcd.concurrency.Session;
import com.coreos.jetcd.concurrency.UMutex;
import com.coreos.jetcd.internal.impl.TestConstants;

public class LockTest {

  private Assertion test;
  private Client client;

  public void setUp() throws Exception {
    test = new Assertion();
    client = ClientBuilder.newBuilder().setEndpoints(TestConstants.endpoints).build();
  }
  
  public void testPut() throws Exception{
    Session session = Session.newBuilder().setClient(client).build();
    Mutex m = new UMutex("a/b/c", session);
    
    if(m.lock()){
      
    }
    
  }
}
