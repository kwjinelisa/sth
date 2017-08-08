package com.coreos.jetcd.concurrency;

import com.coreos.jetcd.data.Response.Header;
import com.coreos.jetcd.op.Op;

public abstract class Mutex {
  protected String pfx;
  protected String key;
  protected long revision;
  protected Session session;
  protected Header header;
  protected boolean isOwner;
  
  public Mutex(String prefix, Session session) {
    this.pfx = prefix;
    this.session = session;
  }

  public long getRev() {
    return this.revision;
  }

  public String getKey() {
    return this.key;
  }
  
  public Session getSession() {
    return this.session;
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
  
  public abstract boolean lock() throws Exception;
  
  public abstract void unlock() throws Exception;
  
  public abstract boolean isOwner();
  
}
