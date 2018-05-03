package com.hubspot.ringleader.watcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.hubspot.ringleader.watcher.Event.Type;

public class PersistentWatcherTest {
  private static final String PATH = "/test";

  private static TestingServer SERVER;

  private Supplier<CuratorFramework> curatorSupplier;
  private PersistentWatcher watcher;
  private List<Event> events;
  private AtomicInteger curatorCounter;

  @BeforeClass
  public static void startup() throws Exception {
    SERVER = new TestingServer();
  }

  @Before
  public void setup() throws Exception {
    createData();
    curatorCounter = new AtomicInteger();
    curatorSupplier = new Supplier<CuratorFramework>() {

      //@Override Java 5 compatibility
      public CuratorFramework get() {
        curatorCounter.incrementAndGet();
        return newCurator();
      }
    };

    events = new CopyOnWriteArrayList<Event>();
    watcher = newWatcher(curatorSupplier);
  }

  @After
  public void cleanup() throws IOException {
    watcher.close();
    deleteData();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    SERVER.close();
  }

  @Test
  public void itStartsUpWithMissingNode() {
    deleteData();
    watcher.start();

    waitForEvents();
    assertThat(curatorCounter.get()).isEqualTo(1);
    assertThat(events).isEmpty();
  }

  @Test
  public void itReplacesBadCurator() {
    final AtomicBoolean exceptionThrown = new AtomicBoolean();
    Supplier<CuratorFramework> exceptionSupplier = new Supplier<CuratorFramework>() {

      //@Override Java 5 compatibility
      public CuratorFramework get() {
        if (!exceptionThrown.get()) {
          exceptionThrown.set(true);
          throw new RuntimeException();
        } else {
          return curatorSupplier.get();
        }
      }
    };

    watcher = newWatcher(exceptionSupplier);
    watcher.start();

    waitForEvents();
    assertThat(curatorCounter.get()).isEqualTo(2);
    assertThat(events).hasSize(1);
    assertThat(events.get(0).getType()).isEqualTo(Type.NODE_UPDATED);
    assertThat(events.get(0).getStat().getVersion()).isEqualTo(0);
    assertThat(events.get(0).getData()).isEqualTo("0".getBytes());
  }

  @Test
  public void itSendsEventWhenNodeIsUpdated() {
    watcher.start();

    waitForEvents();
    assertThat(curatorCounter.get()).isEqualTo(1);
    assertThat(events).hasSize(1);
    assertThat(events.get(0).getType()).isEqualTo(Type.NODE_UPDATED);
    assertThat(events.get(0).getStat().getVersion()).isEqualTo(0);
    assertThat(events.get(0).getData()).isEqualTo("0".getBytes());

    setData("1".getBytes());

    waitForEvents();
    assertThat(curatorCounter.get()).isEqualTo(1);
    assertThat(events).hasSize(2);
    assertThat(events.get(1).getType()).isEqualTo(Type.NODE_UPDATED);
    assertThat(events.get(1).getStat().getVersion()).isEqualTo(1);
    assertThat(events.get(1).getData()).isEqualTo("1".getBytes());
  }

  @Test
  public void itSendsEventWhenNodeIsDeleted() {
    watcher.start();

    waitForEvents();
    assertThat(curatorCounter.get()).isEqualTo(1);
    assertThat(events).hasSize(1);
    assertThat(events.get(0).getType()).isEqualTo(Type.NODE_UPDATED);
    assertThat(events.get(0).getStat().getVersion()).isEqualTo(0);
    assertThat(events.get(0).getData()).isEqualTo("0".getBytes());

    deleteData();

    waitForEvents();
    assertThat(curatorCounter.get()).isEqualTo(1);
    assertThat(events).hasSize(2);
    assertThat(events.get(1).getType()).isEqualTo(Type.NODE_DELETED);
    assertThat(events.get(1).getStat()).isNull();
    assertThat(events.get(1).getData()).isNull();
  }

  @Test
  public void blockingWatcherBlocksUntilTheFirstEventIsSent() {
    final AtomicBoolean exceptionThrown = new AtomicBoolean();
    Supplier<CuratorFramework> delayedSupplier = new Supplier<CuratorFramework>() {

      //@Override Java 5 compatibility
      public CuratorFramework get() {
        if (!exceptionThrown.get()) {
          exceptionThrown.set(true);
          throw new RuntimeException();
        } else {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return curatorSupplier.get();
        }
      }
    };

    watcher = newBlockingWatcher(delayedSupplier);
    watcher.start();

    assertThat(curatorCounter.get()).isEqualTo(2);
    assertThat(events).hasSize(1);
    assertThat(events.get(0).getType()).isEqualTo(Type.NODE_UPDATED);
    assertThat(events.get(0).getStat().getVersion()).isEqualTo(0);
    assertThat(events.get(0).getData()).isEqualTo("0".getBytes());
  }

  private PersistentWatcher newWatcher(Supplier<CuratorFramework> curatorSupplier) {
    PersistentWatcher watcher = new WatcherFactory(curatorSupplier).dataWatcher(PATH);
    addListener(watcher);
    return watcher;
  }

  private PersistentWatcher newBlockingWatcher(Supplier<CuratorFramework> curatorSupplier) {
    PersistentWatcher watcher = new WatcherFactory(curatorSupplier).blockingDataWatcher(PATH);
    addListener(watcher);
    return watcher;
  }

  private void addListener(PersistentWatcher watcher) {
    watcher.getEventListenable().addListener(new EventListener() {

      //@Override Java 5 compatibility
      public void newEvent(Event event) {
        events.add(event);
      }
    });
  }

  private CuratorFramework newCurator() {
    CuratorFramework curator = CuratorFrameworkFactory.builder()
            .connectString(SERVER.getConnectString())
            .sessionTimeoutMs(60000)
            .connectionTimeoutMs(5000)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    curator.start();

    return curator;
  }

  private void createData() {
    CuratorFramework curatorFramework = newCurator();
    try {
      curatorFramework.create().forPath(PATH, "0".getBytes());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      curatorFramework.close();
    }
  }

  private void setData(byte[] newData) {
    CuratorFramework curatorFramework = newCurator();
    try {
      curatorFramework.setData().forPath(PATH, newData);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      curatorFramework.close();
    }
  }

  private void deleteData() {
    CuratorFramework curatorFramework = newCurator();
    try {
      curatorFramework.delete().forPath(PATH);
    } catch (NoNodeException ignored) {
      // ignore
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      curatorFramework.close();
    }
  }

  private void waitForEvents() {
    int size = events.size();
    for (int i = 0; i < 10; i++) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      if (events.size() > size) {
        break;
      }
    }
  }
}
