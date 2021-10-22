package org.apache.crail.storage.active;

import org.apache.crail.storage.StorageServer;
import org.apache.crail.storage.StorageTier;
import org.apache.crail.storage.tcp.TcpStorageClient;

public class ActiveStorageTier extends TcpStorageClient implements StorageTier {
    public StorageServer launchServer () throws Exception {
        return new ActiveStorageServer();
    }
}
