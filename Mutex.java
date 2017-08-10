package com.coreos.jetcd.concurrency;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.Response.Header;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.GetOption.SortOrder;
import com.coreos.jetcd.options.GetOption.SortTarget;

public abstract class Mutex {
  protected String myprefix;
  protected String mykey;
  protected ByteSequence mykkey;
  protected long myrevision;
  protected Session mysession;
  protected Client myclient;
  protected Header header;
  protected boolean isOwner;
  protected KV myKvclient;
    
  protected Mutex(String prefix, Session session, String lockType) {
    myprefix = prefix;
    mysession = session;
    mykey = myprefix + "/" + lockType + "/" + mysession.getLease();
    mykkey = ByteSequence.fromString(mykey);
    myclient = mysession.getClient();
    myKvclient = myclient.getKVClient();
  }

  public static Mutex newUMutex(String prefix, Session session) {
    return new UMutex(prefix, session, "update");
  }
  
  
  public long getRev() {
    return myrevision;
  }

  public String getKey() {
    return mykey;
  }
  
  public Session getSession() {
    return mysession;
  }
  
  public abstract boolean lock() throws Exception;
  
  public abstract void unlock() throws Exception;
  
  public boolean isOwner() {
    return isOwner;
  }
  
  protected Op opWithFirstCreate(String prefix) {
    ByteSequence pprefix = ByteSequence.fromString(prefix);
    return Op.get(pprefix, withFirstCreate(pprefix));
  }
  
  protected GetOption withFirstCreate(ByteSequence prefix) {
    return GetOption.newBuilder().withLimit(1)
        .withSortOrder(SortOrder.ASCEND)
        .withSortField(SortTarget.CREATE)
        .withPrefix(prefix).build();
  }
  
  protected GetOption withLastMaxCreate(ByteSequence prefix, long maxcreateRev) {
    return GetOption.newBuilder().withLimit(1)
        .withSortOrder(SortOrder.DESCEND)
        .withSortField(SortTarget.CREATE)
        .withPrefix(prefix)
        .withMaxCreateRevision(maxcreateRev)
        .build();
  }
  
  protected Op[] concat(Op[] a, Op... b) {
    int alen = a.length;
    int blen = b.length;
    Op[] c = new Op[alen + blen];
    System.arraycopy(a, 0, c, 0, alen);
    System.arraycopy(b, 0, c, alen, blen);
    return c;
  }
  
  protected Op[] concat(Op a, Op[] b) {
    return concat(new Op[]{a}, b);
  }
  
}
