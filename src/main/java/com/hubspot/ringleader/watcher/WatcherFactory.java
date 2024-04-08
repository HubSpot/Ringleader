package com.hubspot.ringleader.watcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatcherFactory {

  private static final Logger LOG = LoggerFactory.getLogger(WatcherFactory.class);
  private final Supplier<CuratorFramework> curatorSupplier;

  private final Object watcherLock;
  private final AtomicReference<CuratorFramework> curatorReference;
  private final AtomicLong curatorTimestamp;
  private final AtomicBoolean replacementScheduled;
  private final AtomicBoolean started;
  private final AtomicBoolean closed;
  private final Map<PersistentWatcher, Boolean> watchers;
  private final ScheduledExecutorService executor;
  private final long replacementIntervalMillis;

  public WatcherFactory(Supplier<CuratorFramework> curatorSupplier) {
    this(curatorSupplier, newExecutor(), TimeUnit.MINUTES.toMillis(1));
  }

  @VisibleForTesting
  WatcherFactory(
    Supplier<CuratorFramework> curatorSupplier,
    ScheduledExecutorService executor,
    long replacementIntervalMillis
  ) {
    this.watcherLock = new Object();
    this.curatorSupplier = curatorSupplier;
    this.curatorReference = new AtomicReference<CuratorFramework>();
    this.curatorTimestamp = new AtomicLong(0);
    this.replacementScheduled = new AtomicBoolean();
    this.started = new AtomicBoolean();
    this.closed = new AtomicBoolean();
    this.watchers = new ConcurrentHashMap<>();
    this.executor = executor;
    this.replacementIntervalMillis = replacementIntervalMillis;
  }

  public PersistentWatcher dataWatcher(String path) {
    synchronized (watcherLock) {
      if (closed.get()) {
        throw new IllegalStateException("This watcher factory has been closed");
      }

      startIfNecessary();

      PersistentWatcher watcher = new PersistentWatcher(this, path);
      watchers.put(watcher, true);
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

      watcher
        .getEventListenable()
        .addListener(
          new EventListener() {
            //@Override Java 5 compatibility
            public void newEvent(Event event) {
              started.countDown();
            }
          }
        );

      watchers.put(watcher, true);
      return watcher;
    }
  }

  AtomicReference<CuratorFramework> getCurator() {
    return curatorReference;
  }

  void recordClose(PersistentWatcher watcher) {
    synchronized (watcherLock) {
      watchers.remove(watcher);
      if (watchers.size() == 0) {
        closed.set(true);
        executor.shutdown();
        synchronousCleanup(curatorReference.get());
      }
    }
  }

  synchronized void replaceCurator() {
    long timestamp = curatorTimestamp.get();
    long age = System.currentTimeMillis() - timestamp;

    if (executor.isShutdown()) {
      // don't reconnect when the executor is shutting down.
      return;
    }

    if (replacementScheduled.get()) {
      return;
    }

    // only attempt reconnect once per replacementIntervalMillis so we don't exacerbate a failure scenario
    if (age < replacementIntervalMillis) {
      if (!replacementScheduled.get()) {
        long millisToWait = replacementIntervalMillis - age;

        replacementScheduled.set(true);
        executor.schedule(
          new Runnable() {
            //@Override Java 5 compatibility
            public void run() {
              replacementScheduled.set(false);
              replaceCurator();
            }
          },
          millisToWait,
          TimeUnit.MILLISECONDS
        );
      }
      return;
    }

    curatorTimestamp.set(System.currentTimeMillis());

    executor.submit(
      new Runnable() {
        //@Override Java 5 compatibility
        public void run() {
          CuratorFramework previous = curatorReference.getAndSet(createNewCurator());
          cleanup(previous);
        }
      }
    );
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

      curator
        .getConnectionStateListenable()
        .addListener(
          new ConnectionStateListener() {
            //@Override Java 5 compatibility
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
              switch (newState) {
                case SUSPENDED:
                case LOST:
                  LOG.info("Connection lost or suspended, replacing client");
                  replaceCurator();
                  break;
                case CONNECTED:
                case RECONNECTED:
                  for (PersistentWatcher watcher : watchers.keySet()) {
                    if (watcher.isStarted()) {
                      watcher.fetchInExecutor();
                    }
                  }
                  break;
                default:
                // make findbugs happy
              }
            }
          }
        );

      curator
        .getUnhandledErrorListenable()
        .addListener(
          new UnhandledErrorListener() {
            //@Override Java 5 compatibility
            public void unhandledError(String message, Throwable e) {
              LOG.error("Curator error, replacing client", e);
              replaceCurator();
            }
          }
        );

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

    executor.submit(
      new Runnable() {
        //@Override Java 5 compatibility
        public void run() {
          synchronousCleanup(curator);
        }
      }
    );
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
    return Executors.newSingleThreadScheduledExecutor(
      new ThreadFactory() {
        //@Override Java 5 compatibility
        public Thread newThread(Runnable r) {
          Thread thread = Executors.defaultThreadFactory().newThread(r);
          thread.setName("WatcherFactoryExecutor");
          thread.setDaemon(true);
          return thread;
        }
      }
    );
  }
}
