package org.apache.crail.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.storage.ActiveEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

/**
 * Client-side channel for streamed access to Crail actions.
 * <p>
 * Common logic for read and write operations.
 */
public abstract class ActiveChannel {
	private static final Logger LOG = CrailUtils.getLogger();

	protected final ActiveEndpoint endpoint;
	protected final BlockInfo block;
	protected final long channelId;
	private final FileInfo fileInfo;
	protected CoreDataStore fs;
	protected CoreObject node;
	protected long position;

	ActiveChannel(CoreObject node, ActiveEndpoint endpoint, BlockInfo block) throws IOException {
		this.node = node;
		this.fs = node.getFileSystem();
		this.fileInfo = node.getFileInfo();
		this.endpoint = endpoint;
		this.block = block;
		this.position = 0;
		try {
			this.channelId = endpoint.openStream(block).get().getChannel();
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException("Could not get a channel ID.", e);
		}
	}

	/**
	 * Run the specific operation for this channel by calling the server endpoint.
	 *
	 * @param operation Representation of the data operation to trigger.
	 * @param buffer    Buffer for the operation data.
	 * @return The Future of the remote invocation.
	 * @throws IOException if there was a problem issuing the remote request.
	 */
	abstract StorageFuture trigger(CoreSubOperation operation, ByteBuffer buffer)
			throws IOException;

	/**
	 * Perform a data operation on this channel with the provided buffer.
	 * <p>
	 * Remote operations have a limit message size specified by the general Crail
	 * block size. If the given buffer is bigger than the block size (buffers are
	 * processed from their position to their limit), the operation is split into
	 * several remote requests. Each remote message contains a buffer at most the
	 * block size. (Message size is supposed to be two times the block size.)
	 *
	 * @param dataBuf Buffer to apply this operation with.
	 * @return The Future object representing this data operation.
	 * @throws IOException if there was a problem with a remote request.
	 */
	final CoreObjectOperation dataOperation(ByteBuffer dataBuf) throws IOException {
		CoreObjectOperation multiOperation = new CoreObjectOperation(dataBuf);

		// split buffer into fragments (max op len = BLOCK_SIZE)
		while (multiOperation.remaining() > 0) {
			int opLen = CrailUtils.minFileBuf(CrailConstants.BLOCK_SIZE, multiOperation.remaining());
			CoreSubOperation subOperation =
					new CoreSubOperation(fileInfo.getFd(), position, multiOperation.getCurrentBufferPosition(), opLen);
			StorageFuture subFuture = this.prepareAndTrigger(subOperation, dataBuf);
			multiOperation.add(subFuture);

			position += opLen;
			multiOperation.incProcessedLen(opLen);
		}
		if (!multiOperation.isProcessed()) {
			throw new IOException("Internal error, processed data != operation length");
		}

		dataBuf.limit(multiOperation.getBufferLimit());
		dataBuf.position(multiOperation.getCurrentBufferPosition());
		return multiOperation;
	}

	private StorageFuture prepareAndTrigger(CoreSubOperation opDesc, ByteBuffer dataBuf)
			throws IOException {
		try {
			dataBuf.clear();
			dataBuf.position(opDesc.getBufferPosition());
			dataBuf.limit(dataBuf.position() + opDesc.getLen());
			return trigger(opDesc, dataBuf);
		} catch (IOException e) {
			LOG.info("ERROR: failed data operation on channel");
			throw e;
		}
	}

}
