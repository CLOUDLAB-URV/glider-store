package org.apache.crail.active;

import java.nio.ByteBuffer;

import org.apache.crail.CrailAction;

/**
 * Sample action implementing a counter.
 */
public class CounterAction extends CrailAction {
	private long count;

	@Override
	public void onCreate() {count = 0;}

	@Override
	public void onDelete() {}

	@Override
	public void onRead(ByteBuffer buffer) {
		buffer.putLong(count);
	}

	@Override
	public int onWrite(ByteBuffer buffer) {
		count += buffer.getLong();
		return Long.BYTES;
	}
}
