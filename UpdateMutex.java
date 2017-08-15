/*
 * Copyright © 2017 CMCC and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package com.coreos.jetcd.concurrency;

import com.coreos.jetcd.op.Op;

public class UpdateMutex extends Mutex {

  protected UpdateMutex(String prefix, Session session, String lockType) {
    super(prefix,  session,  lockType);
  }
  
  protected Op[] otherContendersFirstCreate() {
    return new Op[]{opWithFirstCreate(myprefix + "/update/")};
  }

}
