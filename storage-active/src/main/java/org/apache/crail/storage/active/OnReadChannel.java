package org.apache.crail.storage.active;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;

/**
 * Server-side channel for reading from an action. Actions write to this channel.
 */
public class OnReadChannel implements WritableByteChannel {

	private final BlockingQueue<OperationSlice> data;
	private OperationSlice currentSlice;
	private boolean open;
	private Lock lock;

	public OnReadChannel(BlockingQueue<OperationSlice> dataQueue) {
		this.data = dataQueue;
		this.open = true;
	}

	public OnReadChannel(BlockingQueue<OperationSlice> dataQueue, Lock interleaving) {
		this(dataQueue);
		lock = interleaving;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		if (!open) {
			throw new ClosedChannelException();
		}
		int opLen = 0;
		while (src.hasRemaining()) {
			if (currentSlice == null) {
				takeFromQueue();
			}
			ByteBuffer slice = currentSlice.getSlice();
			if (slice == null) {
				// a null slice here means that the client closed the stream early;
				// close this channel to discard further writes
				open = false;
				return opLen;
			}
			// Copy data from src to the current slice until it is full;
			if (src.remaining() <= slice.remaining()) {
				opLen += src.remaining();
				slice.put(src);
			} else {
				opLen += slice.remaining();
				int oldLimit = src.limit();
				src.limit(src.position() + slice.remaining());
				slice.put(src);
				src.limit(oldLimit);
			}

			// when the current slice gets full, queue it for return
			if (!slice.hasRemaining()) {
				slice.flip();
				currentSlice.complete(slice.remaining());
				currentSlice = null;
			}
		}
		return opLen;
	}

	private void takeFromQueue() throws IOException {
		try {
			if (lock != null) {
				lock.unlock();
			}
			currentSlice = data.take();  // blocking until available
			if (lock != null) {
				lock.lock();
			}
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
		if (!open) {
			return;
		}
		open = false;
		if (currentSlice != null) {
			ByteBuffer slice = currentSlice.getSlice();
			slice.flip();
			currentSlice.complete(slice.remaining());
			currentSlice = null;
		}
		try {
			data.put(new OperationSlice());
			assert (data.size() == 1) : "closed stream with pending slices";
		} catch (InterruptedException e) {
			throw new IOException("could not close stream.", e);
		}
	}
}
