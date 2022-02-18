package org.apache.crail.active;

import org.apache.crail.CrailAction;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Sample action implementation that simply saves what's written and logs all
 * its methods.
 * The streaming methods log the input stream, and generate a random output stream.
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
	public int onWrite(ByteBuffer buffer) {
		System.out.println("Crail action on write");
		myBuffer = ByteBuffer.allocateDirect(buffer.remaining());
		myBuffer.put(buffer);
		myBuffer.rewind();
		return myBuffer.remaining();
	}

	@Override
	public void onWriteStream(InputStream stream) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
//		System.out.println("file lines = " + reader.lines().count());
		reader.lines().forEach(l -> System.out.println(l.substring(1, 50)));
	}

	@Override
	public void onReadStream(OutputStream stream) {
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter((stream)));

		int lowLimit = 48; // numeral '0'
		int highLimit = 122; // letter 'z'
		int lineLen = 10000;
		int nLines = 10;
		Random random = new Random();

		for (int i = 0; i < nLines; i++) {
			String generatedString = random.ints(lowLimit, highLimit + 1)
					.filter(j -> (j <= 57 || j >= 65) && (j <= 90 || j >= 97))
					.limit(lineLen)
					.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
					.toString();
			System.out.println(generatedString.substring(1, 50));
			try {
				writer.write(generatedString);
				writer.newLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
