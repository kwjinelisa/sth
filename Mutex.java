package com.coreos.jetcd.concurrency;

public abstract class Mutex {
  protected String pfx;
  protected String key;
  protected long revision;
  protected Session session;
  
  public Mutex(String prefix, Session session) {
    this.pfx = prefix;
    this.session = session;
  }

  public void setRev(long revision) {
    this.revision = revision;
  }

  public long getRev() {
    return this.revision;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getKey() {
    return this.key;
  }

  public abstract void lock();
  
  public abstract void unlock();
  
  public abstract boolean isOwner();
}
