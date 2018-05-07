package com.hubspot.ringleader.watcher;

import org.apache.zookeeper.data.Stat;

public class Event {
  public enum Type {
    NODE_UPDATED, NODE_DELETED, CONNECTION_LOST, CONNECTED
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

  public static Event connectionLost() {
    return new Event(Type.CONNECTION_LOST, null, null);
  }

  public static Event connected() {
    return new Event(Type.CONNECTED, null, null);
  }

  public Type getType() {
    return type;
  }

  public Stat getStat() {
    return stat;
  }

  public byte[] getData() {
    return data == null ? null : data.clone();
  }
}
