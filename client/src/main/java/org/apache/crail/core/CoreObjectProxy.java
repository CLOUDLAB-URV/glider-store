package org.apache.crail.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.concurrent.TimeUnit;

import org.apache.crail.CrailAction;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.rpc.RpcFuture;
import org.apache.crail.rpc.RpcGetBlock;
import org.apache.crail.storage.ActiveEndpoint;
import org.apache.crail.utils.CrailUtils;
import org.apache.crail.utils.EndpointCache;
import org.slf4j.Logger;

/**
 * A proxy to a {@link  org.apache.crail.CrailObject}.
 */
public class CoreObjectProxy implements CrailObjectProxy {
	private static final Logger LOG = CrailUtils.getLogger();
	private final ActiveEndpoint endpoint;
	private final BlockInfo block;
	protected CoreDataStore fs;
	protected CoreObject node;

	public CoreObjectProxy(CoreObject node) throws Exception {
		this.node = node;
		this.fs = node.getFileSystem();
		FileInfo fileInfo = node.getFileInfo();
		EndpointCache endpointCache = fs.getDatanodeEndpointCache();
		RpcConnection namenodeClientRpc = fs.getNamenodeClientRpc();

		LOG.info("crail proxy: creating");

		// obtain the object's block from namenode; position and capacity are always 0
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
}
