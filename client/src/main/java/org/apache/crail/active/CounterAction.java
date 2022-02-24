package org.apache.crail.active;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

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
	public void onRead(WritableByteChannel channel) {
		OutputStream stream = Channels.newOutputStream(channel);
		DataOutputStream dataOutputStream = new DataOutputStream(stream);
		try {
			dataOutputStream.writeLong(count);
			dataOutputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onWrite(ReadableByteChannel channel) {
		InputStream stream = Channels.newInputStream(channel);
		DataInputStream dataInputStream = new DataInputStream(stream);
		try {
			long delta = dataInputStream.readLong();
			count += delta;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
