package org.apache.crail.storage.active;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;

/**
 * Server-side channel for writing to an action. Actions read from this channel.
 */
public class OnWriteChannel implements ReadableByteChannel {

	private final BlockingQueue<OperationSlice> data;
	private OperationSlice currentSlice;
	private boolean open;
	private Lock lock;

	public OnWriteChannel(BlockingQueue<OperationSlice> dataQueue) {
		this.data = dataQueue;
		this.open = true;
	}

	public OnWriteChannel(BlockingQueue<OperationSlice> dataQueue, Lock interleaving) {
		this(dataQueue);
		lock = interleaving;
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		if (!open) {
			throw new ClosedChannelException();
		}
		if (currentSlice == null) {
			takeFromQueue();
		}
		if (currentSlice.getSlice() == null) {
			// end-of-stream
			return -1;
		}
		ByteBuffer slice = currentSlice.getSlice();
		int opLen = Math.min(dst.remaining(), slice.remaining());
		int oldLimit = slice.limit();
		slice.limit(slice.position() + opLen);
		dst.put(slice);
		slice.limit(oldLimit);

		// when the current slice is fully processed, free it
		if (!slice.hasRemaining()) {
			slice.flip();
			currentSlice.complete(slice.remaining());
			currentSlice = null;
		}
		return opLen;
	}

	private void takeFromQueue() {
		try {
			if (lock != null) {
				lock.unlock();
			}
			currentSlice = data.take();  // blocking until available
			if (lock != null) {
				lock.lock();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			Thread.currentThread().interrupt();
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
