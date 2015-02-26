package com.hubspot.ringleader.watcher;

import com.google.common.base.Supplier;
import com.hubspot.ringleader.watcher.Event.Type;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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

    watcher = new WatcherFactory(curatorSupplier).dataWatcher(PATH);

    events = new CopyOnWriteArrayList<Event>();
    watcher.getEventListenable().addListener(new EventListener() {

      //@Override Java 5 compatibility
      public void newEvent(Event event) {
        events.add(event);
      }
    });
  }

  @After
  public void cleanup() throws IOException {
    deleteData();
    watcher.close();
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
    final Supplier<CuratorFramework> previous = curatorSupplier;
    curatorSupplier = new Supplier<CuratorFramework>() {

      //@Override Java 5 compatibility
      public CuratorFramework get() {
        if (!exceptionThrown.get()) {
          exceptionThrown.set(true);
          throw new RuntimeException();
        } else {
          return previous.get();
        }
      }
    };

    watcher.start();

    waitForEvents();
    assertThat(curatorCounter.get()).isEqualTo(1);
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
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
