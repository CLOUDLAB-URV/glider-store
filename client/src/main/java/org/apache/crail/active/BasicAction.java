package org.apache.crail.active;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Random;

import org.apache.crail.CrailAction;

/**
 * Sample action implementation that simply saves what's written and logs all
 * its methods.
 * The streaming methods log the input stream, and generate a random output stream.
 */
public class BasicAction extends CrailAction {

	@Override
	public void onCreate() {
		System.out.println("Crail action on create");
	}

	@Override
	public void onDelete() {
		System.out.println("Crail action on delete");
	}

	@Override
	public void onWrite(ReadableByteChannel channel) {
		System.out.println("Crail action on write");
		InputStream stream = Channels.newInputStream(channel);
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
//		System.out.println("file lines = " + reader.lines().count());
		reader.lines().forEach(l -> System.out.println(l.substring(1, 50)));
	}

	@Override
	public void onRead(WritableByteChannel channel) {
		System.out.println("Crail action on read");
		OutputStream stream = Channels.newOutputStream(channel);
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
