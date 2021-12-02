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

package org.apache.crail.core;

import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailResult;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.StorageResult;
import org.apache.crail.utils.MultiFuture;

class CoreObjectOperation extends MultiFuture<StorageResult, CrailResult> implements CrailResult {
	private long fileOffset;
	private int bufferPosition;
	private int bufferLimit;
	private int operationLength;

	// current state
	private int inProcessLen;
	private long completedLen;
	private boolean isSynchronous;

	public CoreObjectOperation(CrailBuffer buffer) throws Exception {
		this.bufferPosition = buffer.position();
		this.bufferLimit = buffer.limit();
		this.operationLength = buffer.remaining();
		this.inProcessLen = 0;
		this.completedLen = 0;
		this.isSynchronous = false;

		this.fileOffset = 0;
	}

	public long getLen() {
		return completedLen;
	}

	void incProcessedLen(int opLen) {
		this.inProcessLen += opLen;
	}

	long getInProcessLen() {
		return this.inProcessLen;
	}

	int getBufferLimit() {
		return bufferLimit;
	}

	int remaining() {
		return operationLength - inProcessLen;
	}

	int getCurrentBufferPosition() {
		return bufferPosition + inProcessLen;
	}

	long getCurrentStreamPosition() {
		return fileOffset + inProcessLen;
	}

	boolean isProcessed() {
		return inProcessLen == operationLength;
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	public boolean isCancelled() {
		return false;
	}

	public synchronized void add(StorageFuture dataFuture) {
		super.add(dataFuture);
		if (dataFuture.isSynchronous()) {
			this.isSynchronous = true;
		}
	}

	//-----------

	boolean isSynchronous() {
		return isSynchronous;
	}

	@Override
	public void aggregate(StorageResult result) {
		completedLen += result.getLen();
	}

	@Override
	public CrailResult getAggregate() {
		return this;
	}
}
