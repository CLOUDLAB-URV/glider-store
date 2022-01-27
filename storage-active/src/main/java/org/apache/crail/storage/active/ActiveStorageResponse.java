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

import java.io.IOException;
import java.nio.ByteBuffer;

public class ActiveStorageResponse implements NaRPCMessage {
	public static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES;
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

	public ActiveStorageResponse(CreateResponse emptyResponse) {
		this.emptyResponse = emptyResponse;
		this.type = ActiveStorageProtocol.REQ_CREATE;
		this.error = ActiveStorageProtocol.RET_OK;
	}

	public ActiveStorageResponse(DeleteResponse emptyResponse) {
		this.emptyResponse = emptyResponse;
		this.type = ActiveStorageProtocol.REQ_DEL;
		this.error = ActiveStorageProtocol.RET_OK;
	}

	public ActiveStorageResponse(int type, int error) {
		this.type = type;
		this.error = error;
	}

	public ActiveStorageResponse(int error) {
		this.error = error;
	}

	public int getError() {
		return error;
	}

	public int getType() {
		return type;
	}

	public WriteResponse getWriteResponse() {
		return writeResponse;
	}

	@Override
	public void update(ByteBuffer buffer) throws IOException {
		error = buffer.getInt();
		type = buffer.getInt();
		if (error == ActiveStorageProtocol.RET_OK) {
			if (type == ActiveStorageProtocol.REQ_WRITE) {
				writeResponse.update(buffer);
			} else if (type == ActiveStorageProtocol.REQ_READ) {
				readResponse.update(buffer);
			} else if (type == ActiveStorageProtocol.REQ_CREATE
					| type == ActiveStorageProtocol.REQ_DEL) {
				emptyResponse.update(buffer);
			}
		}
	}

	@Override
	public int write(ByteBuffer buffer) throws IOException {
		buffer.putInt(error);
		buffer.putInt(type);
		int written = HEADER_SIZE;
		if (error == ActiveStorageProtocol.RET_OK) {
			if (type == ActiveStorageProtocol.REQ_WRITE) {
				written += writeResponse.write(buffer);
			} else if (type == ActiveStorageProtocol.REQ_READ) {
				written += readResponse.write(buffer);
			} else if (type == ActiveStorageProtocol.REQ_CREATE
					| type == ActiveStorageProtocol.REQ_DEL) {
				written += emptyResponse.write(buffer);
			}
		}
		return written;
	}

	public static class WriteResponse {
		private int bytesWritten;

		public WriteResponse() {

		}

		public WriteResponse(int bytesWritten) {
			this.bytesWritten = bytesWritten;
		}

		public int getBytesWritten() {
			return bytesWritten;
		}

		public void update(ByteBuffer buffer) throws IOException {
			bytesWritten = buffer.getInt();
		}

		public int write(ByteBuffer buffer) throws IOException {
			buffer.putInt(bytesWritten);
			return 4;
		}
	}

	public static class ReadResponse {

		private final ByteBuffer data;

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
	}

	public static abstract class EmptyResponse {

		public EmptyResponse() {
		}

		public void update(ByteBuffer buffer) throws IOException {
		}

		public int write(ByteBuffer buffer) throws IOException {
			return 0;
		}
	}

	public static class CreateResponse extends EmptyResponse {
	}

	public static class DeleteResponse extends EmptyResponse {
	}

}
