package org.apache.crail.storage.active;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * Representation of a slice in a channel operation.
 */
public class OperationSlice {
	private ByteBuffer slice;
	private int bytesProcessed;
	private CountDownLatch event;

	/**
	 * Create a token for end-of-stream.
	 */
	public OperationSlice() {}

	/**
	 * Create a common slice operation.
	 *
	 * @param src  Buffer where to perform this operation.
	 * @param copy Whether to allocate a new buffer and copy contents
	 *             of {@code src} or use {@code src} directly.
	 */
	public OperationSlice(ByteBuffer src, boolean copy) {
		if (copy) {
			slice = ByteBuffer.allocateDirect(src.remaining());
			slice.put(src).flip();
		} else {
			slice = src;
			event = new CountDownLatch(1);
		}
	}

	/**
	 * Obtain the bytebuffer of this slice.
	 *
	 * @return The buffer; or null if this is and end-of-stream token.
	 */
	public ByteBuffer getSlice() {
		return slice;
	}

	/**
	 * Set this slice as completed.
	 *
	 * @param bytesProcessed Number of bytes that have been processed
	 *                       in this slice.
	 */
	public void complete(int bytesProcessed) {
		this.bytesProcessed = bytesProcessed;
		event.countDown();
	}

	/**
	 * Wait until this slice operation has completed processing.
	 *
	 * @throws InterruptedException if interrupted while waiting.
	 */
	public void waitCompleted() throws InterruptedException {
		event.await();
	}

	/**
	 * @return the number of bytes processed in this slice.
	 */
	public int getBytesProcessed() {
		return bytesProcessed;
	}
}
