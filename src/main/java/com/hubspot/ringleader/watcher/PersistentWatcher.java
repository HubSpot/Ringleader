package com.hubspot.ringleader.watcher;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PersistentWatcher implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PersistentWatcher.class);

  private final Supplier<CuratorFramework> curatorSupplier;
  private final AtomicReference<CuratorFramework> curatorReference;
  private final AtomicLong curatorTimestamp;
  private final AtomicInteger lastVersion;
  private final AtomicBoolean started;
  private final String path;
  private final ScheduledExecutorService executor;
  private final CuratorWatcher watcher;
  private final ListenerContainer<EventListener> listeners;

  PersistentWatcher(Supplier<CuratorFramework> curatorSupplier, final String path) {
    this.curatorSupplier = curatorSupplier;
    this.curatorReference = new AtomicReference<CuratorFramework>();
    this.curatorTimestamp = new AtomicLong(0);
    this.lastVersion = new AtomicInteger(-1);
    this.started = new AtomicBoolean();
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
            fetch(false);
        }
      }
    };
    this.listeners = new ListenerContainer<EventListener>();
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      curatorReference.set(newCurator());
      executor.submit(new Runnable() {

        //@Override Java 5 compatibility
        public void run() {
          try {
            fetch(true);
          } finally {
            executor.schedule(this, 10, TimeUnit.MINUTES);
          }
        }
      });
    }
  }

  public Listenable<EventListener> getEventListenable() {
    return listeners;
  }

  //@Override Java 5 compatibility
  public void close() throws IOException {
    if (started.compareAndSet(true, false)) {
      cleanup(curatorReference.getAndSet(null));
      listeners.clear();
      lastVersion.set(-1);
      curatorTimestamp.set(0);
      executor.shutdown();
    }
  }

  private synchronized void fetch(final boolean backgroundFetch) {
    try {
      CuratorFramework curator = curatorReference.get();
      if (curator == null) {
        LOG.error("No curator present, replacing client");
        replaceCurator();
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

        if (previousVersion != -1 && backgroundFetch) {
          LOG.error("Watcher stopped firing, replacing client");
          replaceCurator();
        }
      }
    } catch (NoNodeException e) {
      LOG.debug("No node exists for path {}", path);
      if (lastVersion.getAndSet(-1) != -1) {
        notifyListeners(Event.nodeDeleted());
      }
    } catch (Exception e) {
      LOG.error("Error fetching data, replacing client", e);
      replaceCurator();
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

  private synchronized void replaceCurator() {
    long timestamp = curatorTimestamp.get();
    long age = System.currentTimeMillis() - timestamp;
    long minAge = TimeUnit.MINUTES.toMillis(1);

    // only attempt reconnect once per minute so we don't exacerbate a failure scenario
    if (age < minAge || !curatorTimestamp.compareAndSet(timestamp, System.currentTimeMillis())) {
      return;
    }

    executor.submit(new Runnable() {

      //@Override Java 5 compatibility
      public void run() {
        CuratorFramework previous = curatorReference.getAndSet(newCurator());
        cleanup(previous);
        fetch(false);
      }
    });
  }

  private CuratorFramework newCurator() {
    CuratorFramework curator = null;
    try {
      curator = curatorSupplier.get();
      if (curator.getState() != CuratorFrameworkState.STARTED) {
        curator.start();
      }

      curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {

        //@Override Java 5 compatibility
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
          switch (newState) {
            case SUSPENDED:
            case LOST:
              LOG.error("Connection lost or suspended, replacing client");
              replaceCurator();
              break;
            default:
              // make findbugs happy
          }
        }
      });

      curator.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {

        //@Override Java 5 compatibility
        public void unhandledError(String message, Throwable e) {
          LOG.error("Curator error, replacing client", e);
          replaceCurator();
        }
      });

      return curator;
    } catch (Exception e) {
      LOG.error("Error creating curator", e);
      cleanup(curator);
      return null;
    }
  }

  private void cleanup(final CuratorFramework curator) {
    if (curator == null) {
      return;
    }

    executor.submit(new Runnable() {

      //@Override Java 5 compatibility
      public void run() {
        try {
          close(curator);
        } catch (Exception e) {
          LOG.debug("Error closing curator", e);
        }
      }

      private void close(CuratorFramework curator) throws Exception {
        try {
          curator.close();
        } catch (UnsupportedOperationException e) {
            /*
            NamespaceFacade throws UnsupportedOperationException when you try to close it
            Need to resort to reflection to access real CuratorFramework instance so we can close it
             */
          Field curatorField = curator.getClass().getDeclaredField("client");
          curatorField.setAccessible(true);

          close((CuratorFramework) curatorField.get(curator));
        }
      }
    });
  }
}
