package com.coreos.jetcd.concurrency;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;

public class UMutex extends Mutex {
  public UMutex(String prefix, Session session) {
    super(prefix, session);
  }

  public void lock() {
    Client client = this.session.getClient();
    key = pfx + session.getLease();
    final Cmp CMP = new Cmp(ByteSequence.fromString(key), Cmp.Op.EQUAL,
    	      CmpTarget.createRevision(0));    
    final Op put = Op.put(ByteSequence.fromString(key), ByteSequence.fromString(""), 
    		PutOption.newBuilder().withLeaseId(session.getLease()).build());
    final Op get = Op.get(ByteSequence.fromString(key), GetOption.DEFAULT);
    final Op getOwner = Op.get(key, option)
    
    
  }
  
  public void unlock() {

  }

  public boolean isOwner() {
    return true;
  }
}
