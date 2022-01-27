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

import com.ibm.narpc.NaRPCFuture;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.StorageResult;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ActiveStorageFuture implements StorageFuture, StorageResult {
	private static final Logger LOG = CrailUtils.getLogger();

	private final NaRPCFuture<ActiveStorageRequest, ActiveStorageResponse> future;
	private int len;
	private ActiveStorageResponse response;

	public ActiveStorageFuture(NaRPCFuture<ActiveStorageRequest, ActiveStorageResponse> future, int len) {
		this.future = future;
		this.len = len;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return future.isDone();
	}

	@Override
	public StorageResult get() throws InterruptedException, ExecutionException {
		response = future.get();
		checkResponse();
		return this;
	}

	private void checkResponse() {
		if (response.getType() == ActiveStorageProtocol.REQ_WRITE) {
			len = response.getWriteResponse().getBytesWritten();
		}
		if (response.getError() != ActiveStorageProtocol.RET_OK) {
			LOG.info("Active storage request returned error. Type: " +
					response.getType() + " Error: " + response.getError());
		}
	}

	@Override
	public StorageResult get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		response = future.get(timeout, unit);
		checkResponse();
		return this;
	}

	@Override
	public boolean isSynchronous() {
		return false;
	}

	@Override
	public int getLen() {
		return len;
	}

}
