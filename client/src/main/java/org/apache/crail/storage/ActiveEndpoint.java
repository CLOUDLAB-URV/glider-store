package org.apache.crail.storage;

import org.apache.crail.metadata.BlockInfo;

import java.io.IOException;

public interface ActiveEndpoint extends StorageEndpoint {
	StorageFuture create(String className, BlockInfo block) throws IOException;
}
