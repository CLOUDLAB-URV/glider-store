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

package org.apache.crail.storage;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.conf.Configurable;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.DataNodeStatistics;
import org.apache.crail.metadata.DataNodeStatus;
import org.apache.crail.rpc.RpcClient;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.rpc.RpcDispatcher;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public interface StorageServer extends Configurable, Runnable {
	public static void main(String[] args) throws Exception {
		final Logger LOG = CrailUtils.getLogger();
		CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
		CrailConstants.updateConstants(conf);
		CrailConstants.printConf();
		CrailConstants.verify();

		int splitIndex = 0;
		for (String param : args) {
			if (param.equalsIgnoreCase("--")) {
				break;
			}
			splitIndex++;
		}

		//default values
		StringTokenizer tokenizer = new StringTokenizer(CrailConstants.STORAGE_TYPES, ",");
		if (!tokenizer.hasMoreTokens()) {
			throw new Exception("No storage types defined!");
		}
		String storageName = tokenizer.nextToken();
		int storageType = 0;
		HashMap<String, Integer> storageTypes = new HashMap<String, Integer>();
		storageTypes.put(storageName, storageType);
		for (int type = 1; tokenizer.hasMoreElements(); type++) {
			String name = tokenizer.nextToken();
			storageTypes.put(name, type);
		}
		int storageClass = -1;

		//custom values
		if (args.length > 0) {
			Option typeOption = Option.builder("t").desc("storage type to start").hasArg().build();
			Option classOption = Option.builder("c").desc("storage class the server will attach to").hasArg().build();
			Options options = new Options();
			options.addOption(typeOption);
			options.addOption(classOption);
			CommandLineParser parser = new DefaultParser();

			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, splitIndex));
				if (line.hasOption(typeOption.getOpt())) {
					storageName = line.getOptionValue(typeOption.getOpt());
					storageType = storageTypes.get(storageName);
				}
				if (line.hasOption(classOption.getOpt())) {
					storageClass = Integer.parseInt(line.getOptionValue(classOption.getOpt()));
				}
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("Storage tier", options);
				System.exit(-1);
			}
		}
		if (storageClass < 0) {
			storageClass = storageType;
		}

		StorageTier storageTier = StorageTier.createInstance(storageName);
		StorageServer server = storageTier.launchServer();

		String[] extraParams = null;
		splitIndex++;
		if (args.length > splitIndex) {
			extraParams = new String[args.length - splitIndex];
			if (args.length - splitIndex >= 0)
				System.arraycopy(args, splitIndex, extraParams, 0, args.length - splitIndex);
		}
		server.init(conf, extraParams);
		server.printConf(LOG);

		Thread serverThread = new Thread(server);
		serverThread.start();

		RpcClient rpcClient = RpcClient.createInstance(CrailConstants.NAMENODE_RPC_TYPE);
		rpcClient.init(conf, args);
		rpcClient.printConf(LOG);

		ConcurrentLinkedQueue<InetSocketAddress> namenodeList = CrailUtils.getNameNodeList();
		ConcurrentLinkedQueue<RpcConnection> connectionList = new ConcurrentLinkedQueue<RpcConnection>();
		while (!namenodeList.isEmpty()) {
			InetSocketAddress address = namenodeList.poll();
			RpcConnection connection = rpcClient.connect(address);
			connectionList.add(connection);
		}
		RpcConnection rpcConnection =
				(connectionList.size() <= 1)
						? connectionList.peek()
						: new RpcDispatcher(connectionList);
		assert rpcConnection != null;
		LOG.info("connected to namenode(s) " + rpcConnection);

		StorageRpcClient storageRpc =
				new StorageRpcClient(storageType, CrailStorageClass.get(storageClass),
									server.getAddress(), rpcConnection);

		HashMap<Long, Long> blockCount = new HashMap<Long, Long>();
		AtomicLong sumCount = new AtomicLong();
		long lba = 0;
		while (true) {
			StorageResource resource = server.allocateResource();
			if (resource == null) {
				break;
			} else {
				storageRpc.setBlock(lba, resource.getAddress(), resource.getLength(), resource.getKey());
				lba += resource.getLength();

				DataNodeStatistics stats = storageRpc.getDataNode();
				long newCount = stats.getFreeBlockCount();
				long serviceId = stats.getServiceId();

				long oldCount = blockCount.getOrDefault(serviceId, 0L);
				long diffCount = newCount - oldCount;
				blockCount.put(serviceId, newCount);
				long count = sumCount.addAndGet(diffCount);
				LOG.info("datanode statistics, allocated freeBlocks " + count);
			}
		}

		CountDownLatch latch = new CountDownLatch(1);

		Runnable keepAliveLoop = () -> {
			try {
				DataNodeStatistics stats = storageRpc.getDataNode();
				long newCount = stats.getFreeBlockCount();
				long serviceId = stats.getServiceId();
				short status = stats.getStatus().getStatus();

				long oldCount = blockCount.getOrDefault(serviceId, 0L);
				long diffCount = newCount - oldCount;
				blockCount.put(serviceId, newCount);
				long count = sumCount.addAndGet(diffCount);

				if (status == DataNodeStatus.STATUS_DATANODE_STOP) {
					// RPC-triggered shutdown
					latch.countDown();
				}
				LOG.info("datanode statistics, freeBlocks " + count);
			} catch (Exception e) {
				e.printStackTrace();
				latch.countDown();
			}
		};

		SignalHandler sh = s -> latch.countDown();
		Signal.handle(new Signal("INT"), sh);
		Signal.handle(new Signal("TERM"), sh);

		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		ScheduledFuture<?> loopFuture = scheduler.scheduleAtFixedRate(
				keepAliveLoop,
				CrailConstants.STORAGE_KEEPALIVE,
				CrailConstants.STORAGE_KEEPALIVE,
				TimeUnit.SECONDS);

		// Await for shutdown
		latch.await();

		// Shutdown
		loopFuture.cancel(false);
		scheduler.shutdown();
		server.prepareToShutDown();
		rpcConnection.close();

		serverThread.interrupt();
	}

	public abstract StorageResource allocateResource() throws Exception;

	public abstract boolean isAlive();

	public abstract void prepareToShutDown();

	public abstract InetSocketAddress getAddress();
}
