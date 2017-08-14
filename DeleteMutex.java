/*
 * Copyright © 2017 CMCC and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package com.coreos.jetcd.concurrency;

import com.coreos.jetcd.op.Op;



public class DeleteMutex extends Mutex {

  protected DeleteMutex(String prefix, Session session, String lockType) {
    super(prefix, session, lockType);
  }
  
  protected Op[] getOtherContendersOp() {
    return new Op[]{opWithFirstCreate(myprefix)};
  }
  
  protected Op[] getContendersImmediatelyBefore(long maxCreateRev) {
    Op[] getDeletes = deletesWithLastMaxCreate(maxCreateRev);
    Op[] others = new Op[]{opWithLastMaxCreate(myprefix, maxCreateRev)};
    return concat(getDeletes, others);
  }


}
