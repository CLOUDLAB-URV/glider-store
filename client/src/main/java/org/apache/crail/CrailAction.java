package org.apache.crail;

import java.nio.ByteBuffer;

public abstract class CrailAction {
	protected CrailStore fs;
	protected CrailObject self;

	private void init(CrailObject node) {
		this.self = node;
		this.fs = node.getFileSystem();
		onCreate();
	}

	public void onCreate() {
	}

	public abstract void onRead(ByteBuffer buffer);

	public abstract void onWrite(ByteBuffer buffer);
}
