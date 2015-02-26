package com.hubspot.ringleader;

import org.apache.zookeeper.data.Stat;

public class Event {
  public enum Type {
    NODE_UPDATED, NODE_DELETED
  }

  private final Type type;
  private final Stat stat;
  private final byte[] data;

  private Event(Type type, Stat stat, byte[] data) {
    this.type = type;
    this.stat = stat;
    this.data = data;
  }

  public static Event nodeUpdated(Stat stat, byte[] data) {
    return new Event(Type.NODE_UPDATED, stat, data);
  }

  public static Event nodeDeleted() {
    return new Event(Type.NODE_DELETED, null, null);
  }

  public Type getType() {
    return type;
  }

  public Stat getStat() {
    return stat;
  }

  public byte[] getData() {
    return data;
  }
}
