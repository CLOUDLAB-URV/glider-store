package org.apache.crail.active;

import org.apache.crail.CrailAction;

import java.nio.ByteBuffer;

/**
 * Sample action implementation that simply saves what's written and logs all
 * its methods.
 */
public class BasicAction extends CrailAction {
	private ByteBuffer myBuffer;

	@Override
	public void onCreate() {
		System.out.println("Crail action on create");
	}

	@Override
	public void onDelete() {
		System.out.println("Crail action on delete");
	}

	@Override
	public void onRead(ByteBuffer buffer) {
		System.out.println("Crail action on read");
		if (myBuffer == null) {
			myBuffer = ByteBuffer.allocateDirect(buffer.remaining());
		}
		myBuffer.clear().limit(buffer.remaining());
		buffer.put(myBuffer);
	}

	@Override
	public void onWrite(ByteBuffer buffer) {
		System.out.println("Crail action on write");
		myBuffer = ByteBuffer.allocateDirect(buffer.remaining());
		myBuffer.put(buffer);
	}
}
