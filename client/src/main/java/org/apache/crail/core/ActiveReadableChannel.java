package org.apache.crail.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.ExecutionException;

import org.apache.crail.CrailResult;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.storage.ActiveEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

/**
 * A client-side channel for reading data from a Crail action.
 * <p>
 * Operations are synchronous.
 */
public final class ActiveReadableChannel extends ActiveChannel implements ReadableByteChannel {
	private static final Logger LOG = CrailUtils.getLogger();
	private boolean open;
	private long totalRead = 0;

	ActiveReadableChannel(CoreObject object, ActiveEndpoint endpoint, BlockInfo block) throws IOException {
		super(object, endpoint, block);
		this.open = true;
		if (CrailConstants.DEBUG) {
			LOG.info("ActiveReadableChannel, open, path " + object.getPath());
		}
	}

	@Override
	StorageFuture trigger(CoreSubOperation operation, ByteBuffer buffer) throws IOException {
		// The fileOffset can determine the order of this operation in the
		// total transfer to the action. Block offset is not relevant.
		return endpoint.readStream(buffer, block, operation.getFileOffset(), channelId);
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		if (!open) {
			throw new ClosedChannelException();
		}
		if (dst.remaining() <= 0) {
			return 0;
		}
		CoreObjectOperation future = dataOperation(dst);
		try {
			CrailResult result = future.get();
			// If multi-operation got end of stream, the aggregation could be < -1
			if (result.getLen() < 0) return -1;
			totalRead += result.getLen();
			return (int) result.getLen();
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException("ActiveChannel:read - Future not completed.", e);
		}
	}

	public long getTotalRead() {
		return totalRead;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	@Override
	public void close() throws IOException {
		if (!open) {
			return;
		}
		// This allows the client to clos the channel before the server responds with end-of-stream.
		// For that, we send finish token to server, so that it discards and stops the operation.
		// If closed after receiving end-of-stream, the server will ignore the token.
		try {
			endpoint.closeRead(block, position, channelId).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException("ActiveChannel:close read - Future not completed.", e);
		}
		open = false;
	}
}
