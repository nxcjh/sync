package com.autohome.sync.syncCluster.tools;

import java.util.List;

public interface PartitionCoordinator {

	List<PartitionManager> getMyManagedPartitions();
}
