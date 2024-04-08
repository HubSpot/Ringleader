package com.hubspot.ringleader.watcher;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Supplier;
import com.google.common.io.Files;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.DirectoryUtils;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReconnectTest {

  private static final int SESSION_TIMEOUT_MS = 5000;
  private static final String PATH = "/test";

  private File sharedDataDirectory = Files.createTempDir();
  private List<TestingServer> servers = new ArrayList<>();
  private List<Event> events = new ArrayList<>();

  private Supplier<CuratorFramework> curatorSupplier;
  private PersistentWatcher watcher;
  private ScheduledExecutorService executor;

  @Before
  public void setup() {
    sharedDataDirectory = Files.createTempDir();

    curatorSupplier =
      new Supplier<CuratorFramework>() {
        //@Override Java 5 compatibility
        public CuratorFramework get() {
          CuratorFramework curator = CuratorFrameworkFactory
            .builder()
            .connectString(getCurrentServer().getConnectString())
            .sessionTimeoutMs(SESSION_TIMEOUT_MS)
            .connectionTimeoutMs(1000)
            .retryPolicy(new ExponentialBackoffRetry(100, 3))
            .build();

          curator.start();

          return curator;
        }
      };

    servers.add(createNewServer());
    createData();

    executor =
      Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).build()
      );

    watcher = new WatcherFactory(curatorSupplier, executor, 5000).dataWatcher(PATH);
    watcher
      .getEventListenable()
      .addListener(
        new EventListener() {
          //@Override Java 5 compatibility
          public void newEvent(Event event) {
            events.add(event);
          }
        }
      );
  }

  @After
  public void after() throws IOException {
    executor.shutdownNow();

    for (TestingServer server : servers) {
      server.stop();
      server.close();
    }

    DirectoryUtils.deleteRecursively(sharedDataDirectory);
  }

  @Test
  public void itReconnectsAfterRepeatedFailures() {
    watcher.start();
    assertWatcherIsRespondingToEvents();

    servers.add(createNewServer());
    assertWatcherIsRespondingToEvents();

    servers.add(createNewServer());
    assertWatcherIsRespondingToEvents();

    servers.add(createNewServer());
    assertWatcherIsRespondingToEvents();
  }

  private void assertWatcherIsRespondingToEvents() {
    events.clear();

    setData(Longs.toByteArray(System.currentTimeMillis()));

    long millisToWait = 10_000L;
    long startedAt = System.currentTimeMillis();
    while (System.currentTimeMillis() < startedAt + millisToWait) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      if (!events.isEmpty()) {
        return;
      }
    }

    throw new RuntimeException("Did not respond to events!");
  }

  private void createData() {
    CuratorFramework curatorFramework = curatorSupplier.get();
    try {
      curatorFramework.create().forPath(PATH, "0".getBytes());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      curatorFramework.close();
    }
  }

  private void setData(byte[] newData) {
    CuratorFramework curatorFramework = curatorSupplier.get();
    try {
      Stat stat = curatorFramework.setData().forPath(PATH, newData);
      stat.getVersion();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      curatorFramework.close();
    }
  }

  private TestingServer createNewServer() {
    try {
      if (!servers.isEmpty()) {
        TestingServer previousServer = getCurrentServer();
        previousServer.stop();
        previousServer.close();
      }

      TestingServer testingServer = new TestingServer(
        new InstanceSpec(sharedDataDirectory, -1, -1, -1, false, -1),
        true
      );
      testingServer.start();

      return testingServer;
    } catch (BindException e) {
      // this can happen due a race when creating TestingServer
      return createNewServer();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private TestingServer getCurrentServer() {
    assertThat(servers.isEmpty())
      .withFailMessage("There are no active servers")
      .isFalse();
    return servers.get(servers.size() - 1);
  }
}
