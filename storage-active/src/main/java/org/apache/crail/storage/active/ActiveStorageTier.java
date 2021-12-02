package org.apache.crail.storage.active;

import org.apache.crail.storage.StorageServer;
import org.apache.crail.storage.StorageTier;

public class ActiveStorageTier extends ActiveStorageClient implements StorageTier {
	public StorageServer launchServer() throws Exception {
		return new ActiveStorageServer();
	}
}
