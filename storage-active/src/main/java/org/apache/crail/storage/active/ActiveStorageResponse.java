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

import static org.apache.crail.storage.active.ActiveStorageRequest.*;

public class ActiveStorageResponse implements NaRPCMessage {
	public static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES;
	public static final int CSIZE = HEADER_SIZE + Math.max(WriteRequest.CSIZE, ReadRequest.CSIZE);

	private int error;
	private int type;
	private WriteResponse writeResponse;
	private ReadResponse readResponse;
	private EmptyResponse emptyResponse;

	public ActiveStorageResponse(WriteResponse writeResponse) {
		this.writeResponse = writeResponse;
		this.type = ActiveStorageProtocol.REQ_WRITE;
		this.error = ActiveStorageProtocol.RET_OK;
	}

	public ActiveStorageResponse(ReadResponse readResponse) {
		this.readResponse = readResponse;
		this.type = ActiveStorageProtocol.REQ_READ;
		this.error = ActiveStorageProtocol.RET_OK;
	}

	public ActiveStorageResponse(EmptyResponse emptyResponse) {
		this.emptyResponse = emptyResponse;
		this.type = ActiveStorageProtocol.REQ_CREATE;
		this.error = ActiveStorageProtocol.RET_OK;
	}

	public ActiveStorageResponse(int error) {
		this.error = error;
	}

	public int size() {
		return CSIZE;
	}

	@Override
	public void update(ByteBuffer buffer) throws IOException {
		error = buffer.getInt();
		type = buffer.getInt();
		if (type == ActiveStorageProtocol.REQ_WRITE) {
			writeResponse.update(buffer);
		} else if (type == ActiveStorageProtocol.REQ_READ) {
			readResponse.update(buffer);
		} else if (type == ActiveStorageProtocol.REQ_CREATE
				| type == ActiveStorageProtocol.REQ_DEL) {
			emptyResponse.update(buffer);
		}
	}

	@Override
	public int write(ByteBuffer buffer) throws IOException {
		buffer.putInt(error);
		buffer.putInt(type);
		int written = HEADER_SIZE;
		if (type == ActiveStorageProtocol.REQ_WRITE) {
			written += writeResponse.write(buffer);
		} else if (type == ActiveStorageProtocol.REQ_READ) {
			written += readResponse.write(buffer);
		} else if (type == ActiveStorageProtocol.REQ_CREATE
				| type == ActiveStorageProtocol.REQ_DEL) {
			written += emptyResponse.write(buffer);
		}
		return written;
	}

	public static class WriteResponse {
		private int size;

		public WriteResponse() {

		}

		public WriteResponse(int size) {
			this.size = size;
		}

		public int size() {
			return size;
		}

		public void update(ByteBuffer buffer) throws IOException {
			size = buffer.getInt();
		}

		public int write(ByteBuffer buffer) throws IOException {
			buffer.putInt(size);
			return 4;
		}
	}

	public static class ReadResponse {
		public static final int CSIZE = Integer.BYTES + (int) CrailConstants.BLOCK_SIZE;

		private ByteBuffer data;

		public ReadResponse(ByteBuffer data) {
			this.data = data;
		}

		public int write(ByteBuffer buffer) throws IOException {
			int written = data.remaining();
			buffer.putInt(data.remaining());
			buffer.put(data);
			return Integer.BYTES + written;
		}

		public void update(ByteBuffer buffer) throws IOException {
			int remaining = buffer.getInt();
			data.clear().limit(remaining);
			buffer.limit(buffer.position() + remaining);
			data.put(buffer);
		}

		public int size() {
			return CSIZE;
		}
	}

	public static class EmptyResponse {

		public EmptyResponse() {
		}

		public void update(ByteBuffer buffer) throws IOException {
		}

		public int write(ByteBuffer buffer) throws IOException {
			return 0;
		}

		public int size() {
			return 0;
		}
	}


}
