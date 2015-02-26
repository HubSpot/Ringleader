package com.hubspot.ringleader;

import com.google.common.base.Supplier;
import com.netflix.curator.framework.CuratorFramework;

public class WatcherFactory {
  private final Supplier<CuratorFramework> curatorSupplier;

  public WatcherFactory(Supplier<CuratorFramework> curatorSupplier) {
    this.curatorSupplier = curatorSupplier;
  }

  public PersistentWatcher dataWatcher(String path) {
    return new PersistentWatcher(curatorSupplier, path);
  }
}
