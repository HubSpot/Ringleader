package com.hubspot.ringleader.watcher;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Supplier;

public class PersistentWatcher implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PersistentWatcher.class);

  private final WatcherFactory parent;
  private final AtomicInteger lastVersion;
  private final AtomicBoolean started;
  private final AtomicBoolean closed;
  private final String path;
  private final ScheduledExecutorService executor;
  private final CuratorWatcher watcher;
  private final ListenerContainer<EventListener> listeners;

  PersistentWatcher(WatcherFactory parent,
                    final String path) {
    this.parent = parent;
    this.lastVersion = new AtomicInteger(-1);
    this.started = new AtomicBoolean();
    this.closed = new AtomicBoolean();
    this.path = path;
    this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

      //@Override Java 5 compatibility
      public Thread newThread(Runnable r) {
        Thread thread = Executors.defaultThreadFactory().newThread(r);
        thread.setName("PersistentWatcher-" + path);
        thread.setDaemon(true);
        return thread;
      }
    });
    // keep a reference to the watcher so we don't add duplicates (Curator uses a set)
    this.watcher = new CuratorWatcher() {

      //@Override Java 5 compatibility
      public void process(WatchedEvent event) throws Exception {
        switch (event.getType()) {
          case NodeDeleted:
            lastVersion.set(-1);
            notifyListeners(Event.nodeDeleted());
            break;
          default:
            fetchInExecutor();
        }
      }
    };
    this.listeners = new ListenerContainer<EventListener>();
  }


  public Supplier<CuratorFramework> getCurator() {
    return new Supplier<CuratorFramework>() {
      @Override
      public CuratorFramework get() {
        return parent.getCurator().get();
      }
    };
  }

  /**
   * Use {@link PersistentWatcher#getCurator()} instead
   *
   * Mutating this value will replace the curator framework for all
   * persistent watchers created by the parent factory of this watcher
   */
  @Deprecated
  public AtomicReference<CuratorFramework> getCuratorReference() {
    return parent.getCurator();
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      fetchInExecutor();
    }
  }

  public boolean isStarted() {
    return started.get();
  }

  public Listenable<EventListener> getEventListenable() {
    return listeners;
  }

  //@Override Java 5 compatibility
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      try {
        listeners.clear();
        lastVersion.set(-1);
        executor.shutdown();
      } finally {
        parent.recordClose(this);
      }
    }
  }

  void fetchInExecutor() {
    executor.submit(new Runnable() {
      //@Override Java 5 compatibility
      public void run() {
        try {
          fetch();
        } finally {
          executor.schedule(this, 10, TimeUnit.MINUTES);
        }
      }
    });
  }

  private synchronized void fetch() {
    try {
      CuratorFramework curator = parent.getCurator().get();
      if (curator == null) {
        LOG.error("No curator present, replacing client");
        parent.replaceCurator();
        return;
      }

      Stat stat = new Stat();
      byte[] data = curator.getData()
              .storingStatIn(stat)
              .usingWatcher(watcher)
              .forPath(path);

      int version = stat.getVersion();
      int previousVersion = lastVersion.getAndSet(version);

      if (version != previousVersion) {
        notifyListeners(Event.nodeUpdated(stat, data));
      }
    } catch (NoNodeException e) {
      LOG.debug("No node exists for path {}", path);
      if (lastVersion.getAndSet(-1) != -1) {
        notifyListeners(Event.nodeDeleted());
      }
    } catch (Exception e) {
      LOG.error("Error fetching data, replacing client", e);
      parent.replaceCurator();
    }
  }

  private void notifyListeners(final Event event) {
    executor.submit(new Runnable() {

      //@Override Java 5 compatibility
      public void run() {
        listeners.forEach(new Function<EventListener, Void>() {

          //@Override Java 5 compatibility
          public Void apply(EventListener listener) {
            listener.newEvent(event);
            return null;
          }
        });
      }
    });
  }
}
