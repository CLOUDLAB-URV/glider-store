package org.apache.crail.active;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.crail.CrailAction;
import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;

/**
 * Sample action implementation that transfers data to a common Crail file.
 */
public class ActionWithFile extends CrailAction {
	private static final String FILE_SUFFIX = "-data";
	private CrailFile myData;

	@Override
	public void onCreate() {
		System.out.println("Crail action on create");

		try {
			myData = this.fs.create(this.self.getPath() + FILE_SUFFIX, CrailNodeType.DATAFILE,
							CrailStorageClass.get(1), CrailLocationClass.DEFAULT, false)
					.get().asFile();
		} catch (Exception e) {
			System.out.println("Error creating data file for action " + self.getPath());
			e.printStackTrace();
		}

	}

	@Override
	public void onDelete() {
		System.out.println("Crail action on delete");
		try {
			this.fs.delete(this.self.getPath() + FILE_SUFFIX, true);
		} catch (Exception e) {
			System.out.println("Error deleting data file for action " + self.getPath());
			e.printStackTrace();
		}
	}

	@Override
	public void onRead(ByteBuffer buffer) {
		System.out.println("Crail action on read");
		try {
			CrailBufferedInputStream bufferedStream =
					myData.getBufferedInputStream(buffer.remaining());
			bufferedStream.read(buffer);
			bufferedStream.close();
		} catch (Exception e) {
			System.out.println("Error reading from data file for action " + self.getPath());
			e.printStackTrace();
		}
	}

	@Override
	public int onWrite(ByteBuffer buffer) {
		System.out.println("Crail action on write");
		try {
			CrailBufferedOutputStream outputStream =
					myData.getBufferedOutputStream(buffer.remaining());
			outputStream.write(buffer);
			outputStream.close();
			return buffer.rewind().remaining();
		} catch (Exception e) {
			System.out.println("Error writing to data file for action " + self.getPath());
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public void onReadStream(WritableByteChannel channel) {
		System.out.println("Crail action with file on read stream");
		try {
			ByteBuffer buffer = ByteBuffer.allocateDirect(100 * 1024);
			CrailBufferedInputStream bufferedStream =
					myData.getBufferedInputStream(100 * 1024);
			while (bufferedStream.read(buffer) != -1) {
				buffer.flip();
				channel.write(buffer);
				buffer.clear();
			}
			bufferedStream.close();
			channel.close();
		} catch (Exception e) {
			System.out.println("Error reading from data file for action " + self.getPath());
			e.printStackTrace();
		}
	}

	@Override
	public void onWriteStream(ReadableByteChannel channel) {
		System.out.println("Crail action with file on write stream");
		try {
			ByteBuffer buffer = ByteBuffer.allocateDirect(100 * 1024);
			CrailBufferedOutputStream outputStream =
					myData.getBufferedOutputStream(100 * 1024);
			while (channel.read(buffer) != -1) {
				buffer.flip();
				outputStream.write(buffer);
				buffer.clear();
			}
			outputStream.close();
			channel.close();
		} catch (Exception e) {
			System.out.println("Error writing to data file for action " + self.getPath());
			e.printStackTrace();
		}
	}
}
