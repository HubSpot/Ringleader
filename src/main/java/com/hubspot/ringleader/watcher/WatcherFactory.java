package com.hubspot.ringleader.watcher;

import com.google.common.base.Supplier;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CountDownLatch;

public class WatcherFactory {
  private final Supplier<CuratorFramework> curatorSupplier;

  public WatcherFactory(Supplier<CuratorFramework> curatorSupplier) {
    this.curatorSupplier = curatorSupplier;
  }

  public PersistentWatcher dataWatcher(String path) {
    return new PersistentWatcher(curatorSupplier, path);
  }

  public PersistentWatcher blockingDataWatcher(String path) {
    final CountDownLatch started = new CountDownLatch(1);

    PersistentWatcher watcher = new PersistentWatcher(curatorSupplier, path) {

      @Override
      public void start() {
        super.start();
        try {
          started.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };

    watcher.getEventListenable().addListener(new EventListener() {

      //@Override Java 5 compatibility
      public void newEvent(Event event) {
        started.countDown();
      }
    });

    return watcher;
  }
}
