package com.hubspot.ringleader.watcher;

import com.google.common.base.Supplier;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class PersistentWatcherTest {
  private static final String PATH = "/test";

  private static TestingServer SERVER;

  private PersistentWatcher watcher;
  private Supplier<CuratorFramework> curatorSupplier;
  private AtomicInteger curatorCounter;

  @BeforeClass
  public static void startup() throws Exception {
    SERVER = new TestingServer();
  }

  @Before
  public void setup() throws Exception {
    curatorCounter = new AtomicInteger();
    curatorSupplier = new Supplier<CuratorFramework>() {

      //@Override Java 5 compatibility
      public CuratorFramework get() {
        curatorCounter.incrementAndGet();
        return CuratorFrameworkFactory.builder()
                .connectString(SERVER.getConnectString())
                .sessionTimeoutMs(60000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
      }
    };

    watcher = new WatcherFactory(curatorSupplier).dataWatcher(PATH);
  }

  @After
  public void cleanup() throws IOException {
    watcher.close();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    SERVER.close();
  }
}
