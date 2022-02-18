package org.apache.crail.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.crail.CrailAction;
import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailResult;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.memory.OffHeapBuffer;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.rpc.RpcFuture;
import org.apache.crail.rpc.RpcGetBlock;
import org.apache.crail.storage.ActiveEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.utils.CrailUtils;
import org.apache.crail.utils.EndpointCache;
import org.slf4j.Logger;

/**
 * A proxy to a {@link  org.apache.crail.CrailObject}.
 */
public class CoreObjectProxy implements CrailObjectProxy {
	private static final Logger LOG = CrailUtils.getLogger();
	private final EndpointCache endpointCache;
	private final RpcConnection namenodeClientRpc;
	private final ActiveEndpoint endpoint;
	private final BlockInfo block;
	private final FileInfo fileInfo;
	protected CoreDataStore fs;
	protected CoreObject node;

	// objects position and capacity is always 0

	public CoreObjectProxy(CoreObject node) throws Exception {
		this.node = node;
		this.fs = node.getFileSystem();
		this.fileInfo = node.getFileInfo();
		this.endpointCache = fs.getDatanodeEndpointCache();
		this.namenodeClientRpc = fs.getNamenodeClientRpc();


		LOG.info("crail proxy: creating");

		// obtain the object's block from namenode
		RpcFuture<RpcGetBlock> rpcFuture =
				namenodeClientRpc.getBlock(fileInfo.getFd(), fileInfo.getToken(), 0, 0);

		RpcGetBlock getBlockRes = rpcFuture.get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		if (!rpcFuture.isDone()) {
			throw new IOException("rpc timeout");
		}
		if (getBlockRes.getError() != RpcErrors.ERR_OK) {
			LOG.info("crail proxy: " + RpcErrors.messages[getBlockRes.getError()]);
			throw new IOException(RpcErrors.messages[getBlockRes.getError()]);
		}
		this.block = getBlockRes.getBlockInfo();

		// get the endpoint to the block's datanode
		try {
			this.endpoint = (ActiveEndpoint) endpointCache.getDataEndpoint(block.getDnInfo());
		} catch (ClassCastException e) {
			throw new Exception("Block of active object is not on an active endpoint.");
		}
	}

	@Override
	public void create(Class<? extends CrailAction> actionClass) throws Exception {
		this.endpoint.create(node.path, actionClass.getName(), this.block).get();
	}

	@Override
	public void delete() throws Exception {
		this.endpoint.delete(this.block).get();
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return Channels.newInputStream(getReadableChannel());
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		return Channels.newOutputStream(getWritableChannel());
	}

	@Override
	public ActiveWritableChannel getWritableChannel() throws IOException {
		return new ActiveWritableChannel(node, endpoint, block);
	}

	@Override
	public ActiveReadableChannel getReadableChannel() throws IOException {
		return new ActiveReadableChannel(node, endpoint, block);
	}


	// TODO: remove these direct operations below -> use streams/channels
	// -------------------------

	public int write(byte[] bytes) throws Exception {
		// Analogous to the buffered stream write
		CrailBuffer buf = bufferOfSize(bytes.length);
		buf.put(bytes).clear();
		return (int) write(buf).get().getLen();
	}

	public Future<CrailResult> write(CrailBuffer dataBuf) throws Exception {
		// Analogous to the direct stream write

		// For now, the operation includes the full buffer, so only one sub-operation.
		CoreObjectOperation multiOperation = new CoreObjectOperation(dataBuf.getByteBuffer());
		int opLen = dataBuf.remaining();
		CoreSubOperation subOperation =
				new CoreSubOperation(fileInfo.getFd(), 0, multiOperation.getCurrentBufferPosition(), opLen);
		multiOperation.incProcessedLen(opLen);

		LOG.info("proxy write: position " + 0 + ", length " + opLen);

		// prepare and trigger
		dataBuf.clear();
		dataBuf.position(subOperation.getBufferPosition());
		dataBuf.limit(dataBuf.position() + subOperation.getLen());
		StorageFuture dataFuture = endpoint.write(dataBuf, block, subOperation.getBlockOffset());
		multiOperation.add(dataFuture);

		if (!multiOperation.isProcessed()) {
			throw new IOException("Internal error, processed data != operation length");
		}

		dataBuf.limit(multiOperation.getBufferLimit());
		dataBuf.position(multiOperation.getCurrentBufferPosition());
		if (multiOperation.isSynchronous()) {
			multiOperation.get();
		}
		return multiOperation;
	}

	public int read(byte[] bytes) throws Exception {
		// Analogous to the buffered stream read
		CrailBuffer buf = bufferOfSize(bytes.length);
		CrailResult result = read(buf).get();
		buf.clear();
		buf.get(bytes, 0, bytes.length);
		return (int) result.getLen();
	}

	@Override
	public Future<CrailResult> read(CrailBuffer dataBuf) throws Exception {
		// Analogous to the direct stream read
		CoreObjectOperation multiOperation = new CoreObjectOperation(dataBuf.getByteBuffer());
		int opLen = dataBuf.remaining();
		CoreSubOperation subOperation =
				new CoreSubOperation(fileInfo.getFd(), 0, multiOperation.getCurrentBufferPosition(), opLen);
		multiOperation.incProcessedLen(opLen);

		LOG.info("proxy read: position " + 0 + ", length " + opLen);

		// prepare and trigger
		dataBuf.clear();
		dataBuf.position(subOperation.getBufferPosition());
		dataBuf.limit(dataBuf.position() + subOperation.getLen());
		StorageFuture dataFuture = endpoint.read(dataBuf, block, subOperation.getBlockOffset());
		multiOperation.add(dataFuture);

		if (!multiOperation.isProcessed()) {
			throw new IOException("Internal error, processed data != operation length");
		}

		dataBuf.limit(multiOperation.getBufferLimit());
		dataBuf.position(multiOperation.getCurrentBufferPosition());
		if (multiOperation.isSynchronous()) {
			multiOperation.get();
		}
		return multiOperation;
	}

	private CrailBuffer bufferOfSize(int size) throws IOException {
		CrailBuffer buf;
		if (size == CrailConstants.BUFFER_SIZE) {
			buf = fs.allocateBuffer();
		} else if (size < CrailConstants.BUFFER_SIZE) {
			buf = fs.allocateBuffer();
			buf.clear().limit(size).slice();
		} else {
			buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
		}
		return buf;
	}
}
