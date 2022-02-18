package org.apache.crail.storage.active;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.BlockingQueue;

/**
 * Server-side channel for writing to an action. Actions read from this channel.
 */
public class OnWriteChannel implements ReadableByteChannel {

	private final BlockingQueue<OperationSlice> data;
	private OperationSlice currentSlice;
	private boolean open;

	public OnWriteChannel(BlockingQueue<OperationSlice> dataQueue) {
		this.data = dataQueue;
		this.open = true;
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		if (!open) {
			throw new ClosedChannelException();
		}
		if (currentSlice == null
				|| (currentSlice.getSlice() != null && !currentSlice.getSlice().hasRemaining())) {
			takeFromQueue();
		}
		if (currentSlice.getSlice() == null) {
			// end-of-stream
			return -1;
		}
		// We suppose buffers on the queue should be fully read,
		// i.e. from position 0 to capacity. And they are added
		// to the queue with position set to 0 and limit to capacity.
		ByteBuffer slice = currentSlice.getSlice();
		int opLen = Math.min(dst.remaining(), slice.remaining());
		slice.limit(slice.position() + opLen);
		dst.put(slice);
		slice.limit(slice.capacity());
		return opLen;
	}

	private void takeFromQueue() throws IOException {
		try {
			currentSlice = data.take();  // blocking until available
		} catch (InterruptedException e) {
			throw new IOException("Interrupted waiting for data.");
		}
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	@Override
	public void close() throws IOException {
		// the action closes the stream early, meaning all further
		// data received should be discarded.
		// TODO: discard rest of data, if any
		this.open = false;
	}
}
