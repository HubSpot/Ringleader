package com.hubspot.ringleader.watcher;

import org.apache.curator.framework.CuratorFramework;

public interface CuratorListener {
  void curatorReplaced(CuratorFramework newCurator);
}
