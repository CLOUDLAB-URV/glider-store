package org.apache.crail.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.crail.metadata.BlockInfo;

public interface ActiveEndpoint extends StorageEndpoint {
	StorageFuture create(String filename, String className, BlockInfo block) throws IOException;

	StorageFuture delete(BlockInfo block) throws IOException;

	StorageFuture writeStream(ByteBuffer buffer, BlockInfo block, long offset, long channelId) throws IOException;

	StorageFuture readStream(ByteBuffer buffer, BlockInfo block, long offset, long channelId) throws IOException;

	StorageFuture openStream(BlockInfo block) throws IOException;

	StorageFuture closeWrite(BlockInfo block, long lastPos, long channelId) throws IOException;

	StorageFuture closeRead(BlockInfo block, long lastPos, long channelId) throws IOException;
}
