package org.apache.crail.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ExecutionException;

import org.apache.crail.CrailResult;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.storage.ActiveEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

/**
 * A client-side channel for writing data to a Crail action.
 * <p>
 * Operations are synchronous.
 */
public final class ActiveWritableChannel extends ActiveChannel implements WritableByteChannel {
	private static final Logger LOG = CrailUtils.getLogger();
	private boolean open;
	private long totalWritten = 0;

	ActiveWritableChannel(CoreObject object, ActiveEndpoint endpoint, BlockInfo block) throws IOException {
		super(object, endpoint, block);
		this.open = true;
		if (CrailConstants.DEBUG) {
			LOG.info("ActiveWritableChannel, open, path " + object.getPath());
		}
	}

	@Override
	StorageFuture trigger(CoreSubOperation operation, ByteBuffer buffer)
			throws IOException {
		// The fileOffset can determine the order of this operation in the
		// total transfer to the action. Block offset is not relevant.
		return endpoint.writeStream(buffer, block, operation.getFileOffset(), channelId);
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		if (!open) {
			throw new ClosedChannelException();
		}
		if (src.remaining() <= 0) {
			return 0;
		}
		CoreObjectOperation future = dataOperation(src);
		try {
			CrailResult result = future.get();
			totalWritten += result.getLen();
			return (int) result.getLen();
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException("ActiveChannel:write - Future not completed.", e);
		}
	}

	public long getTotalWritten() {
		return totalWritten;
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
		// send finish token to server
		try {
			endpoint.closeWrite(block, position, channelId).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException("ActiveChannel:close write - Future not completed.", e);
		}
		open = false;
	}
}
