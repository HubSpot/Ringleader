package com.hubspot.ringleader.watcher;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;

public class WatcherFactory {
  private static final Logger LOG = LoggerFactory.getLogger(WatcherFactory.class);
  private final Supplier<CuratorFramework> curatorSupplier;

  private final Object watcherLock;
  private final AtomicReference<CuratorFramework> curatorReference;
  private final AtomicLong curatorTimestamp;
  private final AtomicBoolean started;
  private final AtomicBoolean closed;
  private final AtomicInteger watchers;
  private final ScheduledExecutorService executor;

  public WatcherFactory(Supplier<CuratorFramework> curatorSupplier) {
    this(curatorSupplier, newExecutor());
  }

  @VisibleForTesting
  WatcherFactory(Supplier<CuratorFramework> curatorSupplier,
                 ScheduledExecutorService executor) {
    this.watcherLock = new Object();
    this.curatorSupplier = curatorSupplier;
    this.curatorReference = new AtomicReference<CuratorFramework>();
    this.curatorTimestamp = new AtomicLong(0);
    this.started = new AtomicBoolean();
    this.closed = new AtomicBoolean();
    this.watchers = new AtomicInteger(0);
    this.executor = executor;
  }

  public PersistentWatcher dataWatcher(String path) {
    synchronized (watcherLock) {
      if (closed.get()) {
        throw new IllegalStateException("This watcher factory has been closed");
      }

      startIfNecessary();

      PersistentWatcher watcher = new PersistentWatcher(this, path);
      watchers.incrementAndGet();
      return watcher;
    }
  }

  public PersistentWatcher blockingDataWatcher(String path) {
    synchronized (watcherLock) {
      if (closed.get()) {
        throw new IllegalStateException("This watcher factory has been closed");
      }

      startIfNecessary();

      final CountDownLatch started = new CountDownLatch(1);

      PersistentWatcher watcher = new PersistentWatcher(this, path) {

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

      watchers.incrementAndGet();
      return watcher;
    }
  }

  AtomicReference<CuratorFramework> getCurator() {
    return curatorReference;
  }

  void recordClose() {
    synchronized (watcherLock) {
      if (watchers.decrementAndGet() == 0) {
        closed.set(true);
        executor.shutdown();
        synchronousCleanup(curatorReference.get());
      }
    }
  }

  synchronized void replaceCurator(final Runnable runnable) {
    long timestamp = curatorTimestamp.get();
    long age = System.currentTimeMillis() - timestamp;
    long minAge = TimeUnit.MINUTES.toMillis(1);

    // only attempt reconnect once per minute so we don't exacerbate a failure scenario and don't reconnect when the
    // executor is shutting down.
    if (age < minAge || !curatorTimestamp.compareAndSet(timestamp, System.currentTimeMillis()) || executor.isShutdown()) {
      return;
    }

    executor.submit(new Runnable() {

      //@Override Java 5 compatibility
      public void run() {
        CuratorFramework previous = curatorReference.getAndSet(createNewCurator());
        cleanup(previous);
        if (runnable != null) {
          runnable.run();
        }
      }
    });
  }

  private void startIfNecessary() {
    if (started.compareAndSet(false, true)) {
      curatorReference.set(createNewCurator());
    }
  }

  private CuratorFramework createNewCurator() {
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
              LOG.warn("Connection lost or suspended, replacing client");
              replaceCurator(null);
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
          replaceCurator(null);
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
        synchronousCleanup(curator);
      }
    });
  }

  private void synchronousCleanup(final CuratorFramework curator) {
    if (curator == null) {
      return;
    }

    try {
      try {
        curator.close();
      } catch (UnsupportedOperationException e) {
        // NamespaceFacade throws UnsupportedOperationException when you try to close it
        // Need to resort to reflection to access real CuratorFramework instance so we can close it
        Field curatorField = curator.getClass().getDeclaredField("client");
        curatorField.setAccessible(true);

        synchronousCleanup((CuratorFramework) curatorField.get(curator));
      }
    } catch (Exception e) {
      LOG.debug("Error closing curator", e);
    }
  }

  private static ScheduledExecutorService newExecutor() {
    return Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      //@Override Java 5 compatibility
      public Thread newThread(Runnable r) {
        Thread thread = Executors.defaultThreadFactory().newThread(r);
        thread.setName("WatcherFactoryExecutor");
        thread.setDaemon(true);
        return thread;
      }
    });
  }
}
