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
import java.nio.ByteBuffer;

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
		// Individual read without channel/stream
		return readStream(buffer.getByteBuffer(), block, offset, -1);
	}

	@Override
	public StorageFuture write(CrailBuffer buffer, BlockInfo block, long offset)
			throws IOException, InterruptedException {
		// Individual write without channel/stream
		return writeStream(buffer.getByteBuffer(), block, offset, -1);
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

	@Override
	public StorageFuture delete(BlockInfo block) throws IOException {
		LOG.info("Active delete, block " + block.getLkey() + "/" + block.getAddr());
		ActiveStorageRequest.DeleteRequest deleteRequest =
				new ActiveStorageRequest.DeleteRequest(block.getLkey(), block.getAddr());
		ActiveStorageResponse.DeleteResponse createResp = new ActiveStorageResponse.DeleteResponse();

		ActiveStorageRequest req = new ActiveStorageRequest(deleteRequest);
		ActiveStorageResponse resp = new ActiveStorageResponse(createResp);

		NaRPCFuture<ActiveStorageRequest, ActiveStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new ActiveStorageFuture(narpcFuture, 0);
	}

	@Override
	public StorageFuture openStream(BlockInfo block) throws IOException {
		LOG.info("Active open channel, "
				+ "block " + block.getLkey() + "/" + block.getAddr());
		ActiveStorageRequest.OpenRequest openRequest =
				new ActiveStorageRequest.OpenRequest(block.getLkey(), block.getAddr());
		ActiveStorageResponse.OpenResponse openResponse = new ActiveStorageResponse.OpenResponse();

		ActiveStorageRequest req = new ActiveStorageRequest(openRequest);
		ActiveStorageResponse resp = new ActiveStorageResponse(openResponse);

		NaRPCFuture<ActiveStorageRequest, ActiveStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new ActiveStorageFuture(narpcFuture, 0);
	}

	@Override
	public StorageFuture writeStream(ByteBuffer buffer, BlockInfo block, long offset, long channelId)
			throws IOException {
		LOG.info("Active write, channel " + channelId + ", buffer " + buffer.remaining()
				+ ", block " + block.getLkey() + "/" + block.getAddr()
				+ ", offset " + offset);
		ActiveStorageRequest.WriteRequest writeReq =
				new ActiveStorageRequest.WriteRequest(block.getLkey(), block.getAddr(), offset,
						buffer.remaining(), buffer, channelId);
		ActiveStorageResponse.WriteResponse writeResp = new ActiveStorageResponse.WriteResponse();

		ActiveStorageRequest req = new ActiveStorageRequest(writeReq);
		ActiveStorageResponse resp = new ActiveStorageResponse(writeResp);

		NaRPCFuture<ActiveStorageRequest, ActiveStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new ActiveStorageFuture(narpcFuture, writeReq.length());
	}

	@Override
	public StorageFuture readStream(ByteBuffer buffer, BlockInfo block, long offset, long channelId)
			throws IOException {
		LOG.info("Active read, channel " + channelId + ", buffer " + buffer.remaining()
				+ ", block " + block.getLkey() + "/" + block.getAddr()
				+ ", offset " + offset);
		ActiveStorageRequest.ReadRequest readReq =
				new ActiveStorageRequest.ReadRequest(block.getLkey(), block.getAddr(), offset, buffer.remaining(), channelId);
		ActiveStorageResponse.ReadResponse readResp = new ActiveStorageResponse.ReadResponse(buffer);

		ActiveStorageRequest req = new ActiveStorageRequest(readReq);
		ActiveStorageResponse resp = new ActiveStorageResponse(readResp);

		NaRPCFuture<ActiveStorageRequest, ActiveStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new ActiveStorageFuture(narpcFuture, readReq.length());
	}

	@Override
	public StorageFuture closeWrite(BlockInfo block, long lastPos, long channelId) throws IOException {
		LOG.info("Active close write, channel " + channelId
				+ ", block " + block.getLkey() + "/" + block.getAddr()
				+ ", lastPos " + lastPos);
		ActiveStorageRequest.CloseRequest closeReq =
				new ActiveStorageRequest.CloseRequest(block.getLkey(), block.getAddr(), channelId, lastPos, true);
		return closeStream(closeReq);
	}

	@Override
	public StorageFuture closeRead(BlockInfo block, long lastPos, long channelId) throws IOException {
		LOG.info("Active close read, channel " + channelId
				+ ", block " + block.getLkey() + "/" + block.getAddr()
				+ ", lastPos " + lastPos);
		ActiveStorageRequest.CloseRequest closeReq =
				new ActiveStorageRequest.CloseRequest(block.getLkey(), block.getAddr(), channelId, lastPos, false);
		return closeStream(closeReq);
	}

	private StorageFuture closeStream(ActiveStorageRequest.CloseRequest closeReq) throws IOException {
		ActiveStorageResponse.CloseResponse closeResp = new ActiveStorageResponse.CloseResponse();

		ActiveStorageRequest req = new ActiveStorageRequest(closeReq);
		ActiveStorageResponse resp = new ActiveStorageResponse(closeResp);

		NaRPCFuture<ActiveStorageRequest, ActiveStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new ActiveStorageFuture(narpcFuture, 0);
	}

}
