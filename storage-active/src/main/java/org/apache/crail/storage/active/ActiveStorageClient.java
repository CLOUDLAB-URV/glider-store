package org.apache.crail.storage.active;

import com.ibm.narpc.NaRPCClientGroup;
import com.ibm.narpc.NaRPCEndpoint;
import org.apache.crail.CrailBufferCache;
import org.apache.crail.CrailStatistics;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.StorageClient;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.tcp.TcpStorageConstants;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;

public class ActiveStorageClient implements StorageClient {
	private NaRPCClientGroup<ActiveStorageRequest, ActiveStorageResponse> clientGroup;

	@Override
	public void init(CrailStatistics statistics, CrailBufferCache bufferCache, CrailConfiguration conf, String[] args)
			throws IOException {
		TcpStorageConstants.updateConstants(conf);

		this.clientGroup = new NaRPCClientGroup<>(
				TcpStorageConstants.STORAGE_TCP_QUEUE_DEPTH,
				(int) CrailConstants.BLOCK_SIZE * 2, false);
	}

	@Override
	public void printConf(Logger logger) {
		TcpStorageConstants.printConf(logger);
	}

	@Override
	public void close() throws Exception {
	}

	@Override
	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		try {
			NaRPCEndpoint<ActiveStorageRequest, ActiveStorageResponse> narpcEndpoint = clientGroup.createEndpoint();
			ActiveStorageEndpoint endpoint = new ActiveStorageEndpoint(narpcEndpoint);
			endpoint.connect(CrailUtils.datanodeInfo2SocketAddr(info));
			return endpoint;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
