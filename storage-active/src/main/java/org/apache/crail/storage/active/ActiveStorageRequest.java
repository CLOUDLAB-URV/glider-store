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

import com.ibm.narpc.NaRPCMessage;
import org.apache.crail.conf.CrailConstants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ActiveStorageRequest implements NaRPCMessage {
	public static final int HEADER_SIZE = Integer.BYTES;
	public static final int CSIZE = HEADER_SIZE + Math.max(WriteRequest.CSIZE, ReadRequest.CSIZE);

	private int type;
	private WriteRequest writeRequest;
	private ReadRequest readRequest;
	private CreateRequest createRequest;

	public ActiveStorageRequest() {
		writeRequest = new WriteRequest();
		readRequest = new ReadRequest();
		createRequest = new CreateRequest();
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

	public int size() {
		return CSIZE;
	}

	public int type() {
		return type;
	}

	@Override
	public void update(ByteBuffer buffer) throws IOException {
		type = buffer.getInt();
		if (type == ActiveStorageProtocol.REQ_WRITE) {
			writeRequest.update(buffer);
		} else if (type == ActiveStorageProtocol.REQ_READ) {
			readRequest.update(buffer);
		} else if (type == ActiveStorageProtocol.REQ_CREATE) {
			createRequest.update(buffer);
		}
	}

	@Override
	public int write(ByteBuffer buffer) throws IOException {
		buffer.putInt(type);
		int written = HEADER_SIZE;
		if (type == ActiveStorageProtocol.REQ_WRITE) {
			written += writeRequest.write(buffer);
		} else if (type == ActiveStorageProtocol.REQ_READ) {
			written += readRequest.write(buffer);
		} else if (type == ActiveStorageProtocol.REQ_CREATE) {
			written += createRequest.write(buffer);
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

	public static class WriteRequest {
		public static final int FIELDS_SIZE = Integer.BYTES + Long.BYTES + Integer.BYTES;
		public static final int CSIZE = FIELDS_SIZE + Integer.BYTES + (int) CrailConstants.BLOCK_SIZE;

		private int key;
		private long address;
		private int length;
		private ByteBuffer data;

		public WriteRequest() {
			data = ByteBuffer.allocateDirect((int) CrailConstants.BLOCK_SIZE);
		}

		public WriteRequest(int key, long address, int length, ByteBuffer buffer) {
			this.key = key;
			this.address = address;
			this.length = length;
			this.data = buffer;
		}

		public long getAddress() {
			return address;
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

		public void update(ByteBuffer buffer) throws IOException {
			key = buffer.getInt();
			address = buffer.getLong();
			length = buffer.getInt();
			int remaining = buffer.getInt();
			buffer.limit(buffer.position() + remaining);
			data.clear();
			data.put(buffer);
			data.flip();
		}

		public int write(ByteBuffer buffer) throws IOException {
			buffer.putInt(key);
			buffer.putLong(address);
			buffer.putInt(length);
			buffer.putInt(data.remaining());
			int written = FIELDS_SIZE + Integer.BYTES + data.remaining();
			buffer.put(data);
			return written;
		}
	}

	public static class ReadRequest {
		public static final int CSIZE = Integer.BYTES + Long.BYTES + Integer.BYTES;

		private int key;
		private long address;
		private int length;

		public ReadRequest() {

		}

		public ReadRequest(int key, long address, int length) {
			this.key = key;
			this.address = address;
			this.length = length;
		}

		public long getAddress() {
			return address;
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

		public void update(ByteBuffer buffer) throws IOException {
			key = buffer.getInt();
			address = buffer.getLong();
			length = buffer.getInt();
		}

		public int write(ByteBuffer buffer) throws IOException {
			buffer.putInt(key);
			buffer.putLong(address);
			buffer.putInt(length);
			return CSIZE;
		}
	}

	public static class CreateRequest {
		public static final int CSIZE = Integer.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;

		private int key;
		private long address;
		private String name;
		private String path;

		public CreateRequest() {

		}

		public CreateRequest(String filename, String className, int key, long address) {
			this.path = filename;
			this.name = className;
			this.key = key;
			this.address = address;
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

		public int size() {
			return CSIZE + name.getBytes(StandardCharsets.UTF_8).length
					+ path.getBytes(StandardCharsets.UTF_8).length;
		}

		public void update(ByteBuffer buffer) throws IOException {
			key = buffer.getInt();
			address = buffer.getLong();
			byte[] nameBytes = new byte[buffer.getInt()];
			buffer.get(nameBytes);
			name = new String(nameBytes, StandardCharsets.UTF_8);
			byte[] pathBytes = new byte[buffer.getInt()];
			buffer.get(pathBytes);
			path = new String(pathBytes, StandardCharsets.UTF_8);
		}

		public int write(ByteBuffer buffer) throws IOException {
			buffer.putInt(key);
			buffer.putLong(address);
			byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
			buffer.putInt(nameBytes.length);
			buffer.put(nameBytes);
			byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
			buffer.putInt(pathBytes.length);
			buffer.put(pathBytes);
			return CSIZE + nameBytes.length + pathBytes.length;
		}
	}

}
