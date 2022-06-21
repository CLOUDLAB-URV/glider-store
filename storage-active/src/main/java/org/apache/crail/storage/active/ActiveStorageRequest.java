/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail.storage.active;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.ibm.narpc.NaRPCMessage;
import org.apache.crail.conf.CrailConstants;

public class ActiveStorageRequest implements NaRPCMessage {
	public static final int HEADER_SIZE = Integer.BYTES;
	public static final int CSIZE = HEADER_SIZE + Math.max(WriteRequest.CSIZE, ReadRequest.CSIZE);

	private int type;
	private WriteRequest writeRequest;
	private ReadRequest readRequest;
	private CreateRequest createRequest;
	private DeleteRequest deleteRequest;
	private OpenRequest openRequest;
	private CloseRequest closeRequest;

	public ActiveStorageRequest() {
		writeRequest = new WriteRequest();
		readRequest = new ReadRequest();
		createRequest = new CreateRequest();
		deleteRequest = new DeleteRequest();
		openRequest = new OpenRequest();
		closeRequest = new CloseRequest();
	}

	public ActiveStorageRequest(WriteRequest writeRequest) {
		this.writeRequest = writeRequest;
		this.type = ActiveStorageProtocol.REQ_WRITE;
	}

	public ActiveStorageRequest(ReadRequest readRequest) {
		this.readRequest = readRequest;
		this.type = ActiveStorageProtocol.REQ_READ;
	}

	public ActiveStorageRequest(CreateRequest createRequest) {
		this.createRequest = createRequest;
		this.type = ActiveStorageProtocol.REQ_CREATE;
	}

	public ActiveStorageRequest(DeleteRequest deleteRequest) {
		this.deleteRequest = deleteRequest;
		this.type = ActiveStorageProtocol.REQ_DEL;
	}

	public ActiveStorageRequest(OpenRequest openRequest) {
		this.openRequest = openRequest;
		this.type = ActiveStorageProtocol.REQ_OPEN;
	}

	public ActiveStorageRequest(CloseRequest closeRequest) {
		this.closeRequest = closeRequest;
		this.type = ActiveStorageProtocol.REQ_CLOSE;
	}

	public int size() {
		return CSIZE;
	}

	public int type() {
		return type;
	}

	@Override
	public void update(ByteBuffer buffer) throws IOException {
		type = buffer.getInt();
		switch (type) {
			case ActiveStorageProtocol.REQ_WRITE:
				writeRequest.update(buffer);
				break;
			case ActiveStorageProtocol.REQ_READ:
				readRequest.update(buffer);
				break;
			case ActiveStorageProtocol.REQ_CREATE:
				createRequest.update(buffer);
				break;
			case ActiveStorageProtocol.REQ_DEL:
				deleteRequest.update(buffer);
				break;
			case ActiveStorageProtocol.REQ_OPEN:
				openRequest.update(buffer);
				break;
			case ActiveStorageProtocol.REQ_CLOSE:
				closeRequest.update(buffer);
				break;
			default:
				throw new IllegalStateException("Unexpected value: " + type);
		}
	}

	@Override
	public int write(ByteBuffer buffer) throws IOException {
		buffer.putInt(type);
		int written = HEADER_SIZE;
		switch (type) {
			case ActiveStorageProtocol.REQ_WRITE:
				written += writeRequest.write(buffer);
				break;
			case ActiveStorageProtocol.REQ_READ:
				written += readRequest.write(buffer);
				break;
			case ActiveStorageProtocol.REQ_CREATE:
				written += createRequest.write(buffer);
				break;
			case ActiveStorageProtocol.REQ_DEL:
				written += deleteRequest.write(buffer);
				break;
			case ActiveStorageProtocol.REQ_OPEN:
				written += openRequest.write(buffer);
				break;
			case ActiveStorageProtocol.REQ_CLOSE:
				written += closeRequest.write(buffer);
				break;
			default:
				throw new IllegalStateException("Unexpected value: " + type);
		}
		return written;
	}

	public WriteRequest getWriteRequest() {
		return writeRequest;
	}

	public ReadRequest getReadRequest() {
		return readRequest;
	}

	public CreateRequest getCreateRequest() {
		return createRequest;
	}

	public DeleteRequest getDeleteRequest() {
		return deleteRequest;
	}

	public OpenRequest getOpenRequest() {
		return openRequest;
	}

	public CloseRequest getCloseRequest() {
		return closeRequest;
	}

	public static class WriteRequest {
		public static final int FIELDS_SIZE = Integer.BYTES + 3 * Long.BYTES + Integer.BYTES;
		public static final int CSIZE = FIELDS_SIZE + Integer.BYTES + (int) CrailConstants.BLOCK_SIZE;

		private int key;
		private long address;
		private long offset;
		private long channel;
		private int length;
		private ByteBuffer data;

		public WriteRequest() {
			data = ByteBuffer.allocateDirect((int) CrailConstants.BLOCK_SIZE);
		}

		public WriteRequest(int key, long address, long offset, int length, ByteBuffer buffer, long channel) {
			this.key = key;
			this.address = address;
			this.offset = offset;
			this.length = length;
			this.data = buffer;
			this.channel = channel;
		}

		public long getAddress() {
			return address;
		}

		public long getOffset() {
			return offset;
		}

		public long getChannel() {
			return channel;
		}

		public int length() {
			return length;
		}

		public int getKey() {
			return key;
		}

		public ByteBuffer getBuffer() {
			return data;
		}

		public int size() {
			return CSIZE;
		}

		public void update(ByteBuffer buffer) {
			key = buffer.getInt();
			address = buffer.getLong();
			offset = buffer.getLong();
			channel = buffer.getLong();
			length = buffer.getInt();
			int remaining = buffer.getInt();
			buffer.limit(buffer.position() + remaining);
			data.clear();
			data.put(buffer);
			data.flip();
		}

		public int write(ByteBuffer buffer) {
			buffer.putInt(key);
			buffer.putLong(address);
			buffer.putLong(offset);
			buffer.putLong(channel);
			buffer.putInt(length);
			buffer.putInt(data.remaining());
			int written = FIELDS_SIZE + Integer.BYTES + data.remaining();
			buffer.put(data);
			return written;
		}
	}

	public static class ReadRequest {
		public static final int CSIZE = Integer.BYTES + 3 * Long.BYTES + Integer.BYTES;

		private int key;
		private long address;
		private long offset;
		private long channel;
		private int length;

		public ReadRequest() {

		}

		public ReadRequest(int key, long address, long offset, int length, long channel) {
			this.key = key;
			this.address = address;
			this.offset = offset;
			this.length = length;
			this.channel = channel;
		}

		public long getAddress() {
			return address;
		}

		public long getOffset() {
			return offset;
		}

		public long getChannel() {
			return channel;
		}

		public int length() {
			return length;
		}

		public int getKey() {
			return key;
		}

		public int size() {
			return CSIZE;
		}

		public void update(ByteBuffer buffer) {
			key = buffer.getInt();
			address = buffer.getLong();
			offset = buffer.getLong();
			channel = buffer.getLong();
			length = buffer.getInt();
		}

		public int write(ByteBuffer buffer) {
			buffer.putInt(key);
			buffer.putLong(address);
			buffer.putLong(offset);
			buffer.putLong(channel);
			buffer.putInt(length);
			return CSIZE;
		}
	}

	public static class CreateRequest {
		public static final int CSIZE = Integer.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES + Character.BYTES;

		private int key;
		private long address;
		private String name;
		private String path;
		private boolean interleaving;

		public CreateRequest() {
		}

		public CreateRequest(String filename, String className, int key, long address) {
			this.path = filename;
			this.name = className;
			this.key = key;
			this.address = address;
			this.interleaving = false;
		}

		public CreateRequest(String filename, String className, int key, long address, boolean interleaving) {
			this.path = filename;
			this.name = className;
			this.key = key;
			this.address = address;
			this.interleaving = interleaving;
		}

		public long getAddress() {
			return address;
		}

		public int getKey() {
			return key;
		}

		public String getName() {
			return name;
		}

		public String getPath() {
			return path;
		}

		public boolean getInterleaving() {
			return interleaving;
		}

		public int size() {
			return CSIZE + name.getBytes(StandardCharsets.UTF_8).length
					+ path.getBytes(StandardCharsets.UTF_8).length;
		}

		public void update(ByteBuffer buffer) {
			key = buffer.getInt();
			address = buffer.getLong();
			interleaving = buffer.getChar() == 't';
			byte[] nameBytes = new byte[buffer.getInt()];
			buffer.get(nameBytes);
			name = new String(nameBytes, StandardCharsets.UTF_8);
			byte[] pathBytes = new byte[buffer.getInt()];
			buffer.get(pathBytes);
			path = new String(pathBytes, StandardCharsets.UTF_8);
		}

		public int write(ByteBuffer buffer) {
			buffer.putInt(key);
			buffer.putLong(address);
			if (interleaving) {
				buffer.putChar('t');
			} else {
				buffer.putChar('f');
			}
			byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
			buffer.putInt(nameBytes.length);
			buffer.put(nameBytes);
			byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
			buffer.putInt(pathBytes.length);
			buffer.put(pathBytes);
			return CSIZE + nameBytes.length + pathBytes.length;
		}
	}

	public static class DeleteRequest {
		public static final int CSIZE = Integer.BYTES + Long.BYTES;

		private int key;
		private long address;

		public DeleteRequest() {

		}

		public DeleteRequest(int key, long address) {
			this.key = key;
			this.address = address;
		}

		public long getAddress() {
			return address;
		}

		public int getKey() {
			return key;
		}

		public int size() {
			return CSIZE;
		}

		public void update(ByteBuffer buffer) {
			key = buffer.getInt();
			address = buffer.getLong();
		}

		public int write(ByteBuffer buffer) {
			buffer.putInt(key);
			buffer.putLong(address);
			return CSIZE;
		}
	}

	public static class OpenRequest {
		public static final int CSIZE = Integer.BYTES + Long.BYTES;

		private int key;
		private long address;

		public OpenRequest() {
		}

		public OpenRequest(int key, long address) {
			this.key = key;
			this.address = address;
		}

		public long getAddress() {
			return address;
		}

		public int getKey() {
			return key;
		}

		public int size() {
			return CSIZE;
		}

		public void update(ByteBuffer buffer) {
			key = buffer.getInt();
			address = buffer.getLong();
		}

		public int write(ByteBuffer buffer) {
			buffer.putInt(key);
			buffer.putLong(address);
			return CSIZE;
		}
	}

	public static class CloseRequest {
		public static final int CSIZE = Integer.BYTES + 3 * Long.BYTES + Short.BYTES;

		private int key;
		private long address;
		private long channel;
		private long total;
		private short isWrite;

		public CloseRequest() {
		}

		public CloseRequest(int key, long address, long channel, long total, boolean isWrite) {
			this.key = key;
			this.address = address;
			this.channel = channel;
			this.total = total;
			this.isWrite = (short) (isWrite ? 0 : 1);
		}

		public long getAddress() {
			return address;
		}

		public int getKey() {
			return key;
		}

		public long getChannel() {
			return channel;
		}

		public long getTotal() {
			return total;
		}

		public boolean isWrite() {
			return isWrite == 0;
		}

		public int size() {
			return CSIZE;
		}

		public void update(ByteBuffer buffer) {
			key = buffer.getInt();
			address = buffer.getLong();
			channel = buffer.getLong();
			total = buffer.getLong();
			isWrite = buffer.getShort();
		}

		public int write(ByteBuffer buffer) {
			buffer.putInt(key);
			buffer.putLong(address);
			buffer.putLong(channel);
			buffer.putLong(total);
			buffer.putShort(isWrite);
			return CSIZE;
		}
	}

}
