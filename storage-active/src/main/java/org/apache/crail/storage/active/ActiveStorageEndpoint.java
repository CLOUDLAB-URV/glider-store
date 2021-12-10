package org.apache.crail.storage.active;

import com.ibm.narpc.NaRPCEndpoint;
import com.ibm.narpc.NaRPCFuture;
import org.apache.crail.CrailBuffer;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.storage.ActiveEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ActiveStorageEndpoint implements ActiveEndpoint {
	private static final Logger LOG = CrailUtils.getLogger();
	private final NaRPCEndpoint<ActiveStorageRequest, ActiveStorageResponse> endpoint;

	public ActiveStorageEndpoint(NaRPCEndpoint<ActiveStorageRequest, ActiveStorageResponse> endpoint) {
		this.endpoint = endpoint;
	}

	public void connect(InetSocketAddress address) throws IOException {
		endpoint.connect(address);
	}

	@Override
	public void close() throws IOException, InterruptedException {
		endpoint.close();
	}

	@Override
	public boolean isLocal() {
		return false;
	}

	@Override
	public StorageFuture read(CrailBuffer buffer, BlockInfo block, long offset)
			throws IOException, InterruptedException {
		LOG.info("Active read, buffer " + buffer.remaining() + ", block " + block.getLkey() + "/"
				+ block.getAddr() + "/" + block.getLength() + ", offset " + offset);
		ActiveStorageRequest.ReadRequest readReq =
				new ActiveStorageRequest.ReadRequest(block.getLkey(), block.getAddr() + offset, buffer.remaining());
		ActiveStorageResponse.ReadResponse readResp = new ActiveStorageResponse.ReadResponse(buffer.getByteBuffer());

		ActiveStorageRequest req = new ActiveStorageRequest(readReq);
		ActiveStorageResponse resp = new ActiveStorageResponse(readResp);

		NaRPCFuture<ActiveStorageRequest, ActiveStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new ActiveStorageFuture(narpcFuture, readReq.length());
	}

	@Override
	public StorageFuture write(CrailBuffer buffer, BlockInfo block, long offset)
			throws IOException, InterruptedException {
		LOG.info("Active write, buffer " + buffer.remaining() + ", block " + block.getLkey() + "/"
				+ block.getAddr() + "/" + block.getLength() + ", offset " + offset);
		ActiveStorageRequest.WriteRequest writeReq =
				new ActiveStorageRequest.WriteRequest(block.getLkey(), block.getAddr() + offset,
						buffer.remaining(), buffer.getByteBuffer());
		ActiveStorageResponse.WriteResponse writeResp = new ActiveStorageResponse.WriteResponse();

		ActiveStorageRequest req = new ActiveStorageRequest(writeReq);
		ActiveStorageResponse resp = new ActiveStorageResponse(writeResp);

		NaRPCFuture<ActiveStorageRequest, ActiveStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new ActiveStorageFuture(narpcFuture, writeReq.length());
	}

	@Override
	public StorageFuture create(String filename, String className, BlockInfo block) throws IOException {
		LOG.info("Active create, class name " + className + ", block " + block.getLkey() + "/" + block.getAddr());
		ActiveStorageRequest.CreateRequest createReq =
				new ActiveStorageRequest.CreateRequest(filename, className, block.getLkey(), block.getAddr());
		ActiveStorageResponse.CreateResponse createResp = new ActiveStorageResponse.CreateResponse();

		ActiveStorageRequest req = new ActiveStorageRequest(createReq);
		ActiveStorageResponse resp = new ActiveStorageResponse(createResp);

		NaRPCFuture<ActiveStorageRequest, ActiveStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new ActiveStorageFuture(narpcFuture, 0);
	}
}
