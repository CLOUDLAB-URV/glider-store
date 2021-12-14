package org.apache.crail.core;

import org.apache.crail.CrailAction;
import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailResult;
import org.apache.crail.conf.CrailConstants;
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

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CoreObjectProxy implements CrailObjectProxy {
	private static final Logger LOG = CrailUtils.getLogger();
	private final EndpointCache endpointCache;
	private final RpcConnection namenodeClientRpc;
	private final ActiveEndpoint endpoint;
	private final BlockInfo block;
	private final FileInfo fileInfo;
	protected CoreDataStore fs;
	protected CoreNode node;
	private long position;
	private long syncedCapacity;

	public CoreObjectProxy(CoreObject node) throws Exception {
		this.node = node;
		this.fs = node.getFileSystem();
		this.fileInfo = node.getFileInfo();
		this.endpointCache = fs.getDatanodeEndpointCache();
		this.namenodeClientRpc = fs.getNamenodeClientRpc();

		this.position = 0;
		this.syncedCapacity = 0;

		LOG.info("crail proxy: position " + position + ", syncCapacity " + syncedCapacity);

		// obtain the object's block from namenode
		RpcFuture<RpcGetBlock> rpcFuture =
				namenodeClientRpc.getBlock(fileInfo.getFd(), fileInfo.getToken(), position, syncedCapacity);

		RpcGetBlock getBlockRes = rpcFuture.get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		if (!rpcFuture.isDone()) {
			throw new IOException("rpc timeout");
		}
		if (getBlockRes.getError() != RpcErrors.ERR_OK) {
			LOG.info("inputStream: " + RpcErrors.messages[getBlockRes.getError()]);
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

	public void write(byte[] bytes) throws Exception {
		// Analogous to the buffered stream write
		CrailBuffer buffer = fs.allocateBuffer().clear().limit(bytes.length).slice();
		buffer.put(bytes).clear();
		write(buffer).get();
	}

	public Future<CrailResult> write(CrailBuffer dataBuf) throws Exception {
		// Analogous to the direct stream write

		// For now, the operation includes the full buffer, so only one sub-operation.
		// FIXME: It should enable multi-operations in the future.
		CoreObjectOperation multiOperation = new CoreObjectOperation(dataBuf);
		int opLen = dataBuf.remaining();
		CoreSubOperation subOperation =
				new CoreSubOperation(fileInfo.getFd(), position, multiOperation.getCurrentBufferPosition(), opLen);
		multiOperation.incProcessedLen(opLen);

		LOG.info("proxy write: position " + position + ", length " + opLen);

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
		CrailBuffer buffer = fs.allocateBuffer().clear().limit(bytes.length).slice();
		CrailResult result = read(buffer).get();
		buffer.clear();
		buffer.get(bytes, 0, bytes.length);
		return (int) result.getLen();
	}

	@Override
	public Future<CrailResult> read(CrailBuffer dataBuf) throws Exception {
		// Analogous to the direct stream read
		CoreObjectOperation multiOperation = new CoreObjectOperation(dataBuf);
		int opLen = dataBuf.remaining();
		CoreSubOperation subOperation =
				new CoreSubOperation(fileInfo.getFd(), position, multiOperation.getCurrentBufferPosition(), opLen);
		multiOperation.incProcessedLen(opLen);

		LOG.info("proxy read: position " + position + ", length " + opLen);

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
}
