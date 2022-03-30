package org.apache.crail.storage.active;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ibm.narpc.NaRPCServerChannel;
import com.ibm.narpc.NaRPCServerEndpoint;
import com.ibm.narpc.NaRPCServerGroup;
import com.ibm.narpc.NaRPCService;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.storage.StorageResource;
import org.apache.crail.storage.StorageServer;
import org.apache.crail.storage.StorageUtils;
import org.apache.crail.storage.tcp.TcpStorageConstants;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class ActiveStorageServer implements StorageServer, NaRPCService<ActiveStorageRequest, ActiveStorageResponse> {
	private static final Logger LOG = CrailUtils.getLogger();

	private NaRPCServerGroup<ActiveStorageRequest, ActiveStorageResponse> serverGroup;
	private NaRPCServerEndpoint<ActiveStorageRequest, ActiveStorageResponse> serverEndpoint;
	private InetSocketAddress address;
	private boolean alive;
	private long regions;
	private long keys;
	private ConcurrentHashMap<Integer, ActionManager> actionManagers; // one manager per region
	private HashMap<Long, ByteBuffer> readBuffers;

	private ScheduledExecutorService scheduler;
	private List<String> jars;
	private CrailStore fs;

	public static synchronized void loadLibrary(java.io.File jar) {
		try {
			LOG.info("Loading JAR: " + jar.getName());
			ClassLoader classLoader = ClassLoader.getSystemClassLoader();
			try {
				Method method = classLoader.getClass().getDeclaredMethod("addURL", URL.class);
				method.setAccessible(true);
				method.invoke(classLoader, jar.toURI().toURL());
			} catch (NoSuchMethodException e) {
				Method method = classLoader.getClass()
						.getDeclaredMethod("appendToClassPathForInstrumentation", String.class);
				method.setAccessible(true);
				method.invoke(classLoader, jar.getPath());
			}
		} catch (final java.lang.NoSuchMethodException | java.lang.IllegalAccessException
				| java.net.MalformedURLException | java.lang.reflect.InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void init(CrailConfiguration conf, String[] args) throws Exception {
		TcpStorageConstants.init(conf, args);
		ActiveStorageConstants.init(conf, args);

		this.serverGroup = new NaRPCServerGroup<>(this,
				TcpStorageConstants.STORAGE_TCP_QUEUE_DEPTH,
				(int) CrailConstants.BLOCK_SIZE * 2,
				false,
				ActiveStorageConstants.STORAGE_ACTIVE_CORES);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		this.address = StorageUtils.getDataNodeAddress(TcpStorageConstants.STORAGE_TCP_INTERFACE,
				TcpStorageConstants.STORAGE_TCP_PORT);
		serverEndpoint.bind(address);
		this.alive = false;
		this.regions = TcpStorageConstants.STORAGE_TCP_STORAGE_LIMIT / TcpStorageConstants.STORAGE_TCP_ALLOCATION_SIZE;
		this.keys = 0;
		this.actionManagers = new ConcurrentHashMap<>();
		this.readBuffers = new HashMap<>();

		scheduler = Executors.newScheduledThreadPool(1);
		jars = new LinkedList<>();

		fs = CrailStore.newInstance(conf);
	}

	@Override
	public void printConf(Logger logger) {
		TcpStorageConstants.printConf(logger);
		ActiveStorageConstants.printConf(logger);
	}

	@Override
	public StorageResource allocateResource() throws Exception {
		StorageResource resource = null;
		if (keys < regions) {
			int regionKey = (int) keys++;
			actionManagers.put(regionKey, new ActionManager(fs));
			// Object capacity per region is the number of blocks it fits
			resource = StorageResource.createResource(0,
					(int) TcpStorageConstants.STORAGE_TCP_ALLOCATION_SIZE,
					regionKey);
		}
		return resource;
	}

	@Override
	public InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public boolean isAlive() {
		return alive;
	}

	@Override
	public void prepareToShutDown() {
		LOG.info("Preparing Active-Storage server for shutdown");
		this.alive = false;

		try {
			scheduler.shutdown();
			serverEndpoint.close();
			serverGroup.close();
			actionManagers.forEach((k, v) -> v.close());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		LOG.info("running Active-TCP storage server, address " + address);
		this.alive = true;

		Runnable jarLoader = () -> {
			try {
				File folder = new File(ActiveStorageConstants.STORAGE_ACTIVE_JAR_DIR);
				LOG.info("Monitoring jars in " + ActiveStorageConstants.STORAGE_ACTIVE_JAR_DIR);
				File[] listOfFiles = folder.listFiles();
				if (listOfFiles == null) {
					LOG.error("User JAR path is not a directory!");
					return;
				}
				for (File file : listOfFiles) {
					if (file.isFile() && file.getName().matches(".*\\.jar") && !jars.contains(file.getName())) {
						loadLibrary(file);
						jars.add(file.getName());
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

		try {
			jarLoader.run();
		} catch (Exception e) {
			// ignore
		}

		scheduler.scheduleAtFixedRate(jarLoader, 10, 10, TimeUnit.SECONDS);

		try {
			while (true) {
				NaRPCServerChannel endpoint = serverEndpoint.accept();
				LOG.info("new connection " + endpoint.address());
			}
		} catch (Exception e) {
			// if StorageServer is still marked as running, output stacktrace;
			// otherwise this is expected behaviour
			if (this.alive) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public ActiveStorageRequest createRequest() {
		return new ActiveStorageRequest();
	}

	@Override
	public ActiveStorageResponse processRequest(ActiveStorageRequest request) {
		switch (request.type()) {
			case ActiveStorageProtocol.REQ_CREATE: {
				ActiveStorageRequest.CreateRequest createRequest = request.getCreateRequest();
				// LOG.info("processing active create request, key " + createRequest.getKey()
				// 		+ ", address " + createRequest.getAddress() + ", action class " + createRequest.getName());

				try {
					actionManagers.get(createRequest.getKey())
							.create(createRequest.getAddress(), createRequest.getName(),
							        createRequest.getPath(), createRequest.getInterleaving());
					ActiveStorageResponse.CreateResponse createResponse = new ActiveStorageResponse.CreateResponse();
					return new ActiveStorageResponse(createResponse);
				} catch (NoActionException e) {
					return new ActiveStorageResponse(ActiveStorageProtocol.REQ_CREATE,
							ActiveStorageProtocol.RET_NOT_CREATED);
				}
			}
			case ActiveStorageProtocol.REQ_WRITE: {
				ActiveStorageRequest.WriteRequest writeRequest = request.getWriteRequest();
				// LOG.info("processing active write request, key " + writeRequest.getKey()
				// 		+ ", address " + writeRequest.getAddress() + ", length " + writeRequest.length()
				// 		+ ", offset " + writeRequest.getOffset()
				// 		+ ", channel " + writeRequest.getChannel());

				try {
					// TODO: read and write requests include offsets in case it is necessary ordering them.
					//  It is not needed now because clients perform channel operations synchronously.
					int written = actionManagers.get(writeRequest.getKey())
							.write(writeRequest.getAddress(), writeRequest.getBuffer(), writeRequest.getChannel());
					ActiveStorageResponse.WriteResponse writeResponse =
							new ActiveStorageResponse.WriteResponse(written);
					return new ActiveStorageResponse(writeResponse);
				} catch (NoActionException e) {
					return new ActiveStorageResponse(ActiveStorageProtocol.REQ_WRITE,
							ActiveStorageProtocol.RET_NOT_CREATED);
				}
			}
			case ActiveStorageProtocol.REQ_READ: {
				ActiveStorageRequest.ReadRequest readRequest = request.getReadRequest();
				// LOG.info("processing active read request, address " + readRequest.getAddress()
				// 		+ ", length " + readRequest.length()
				// 		+ ", offset " + readRequest.getOffset()
				// 		+ ", channel " + readRequest.getChannel());
				ByteBuffer data = readBuffers.computeIfAbsent(Thread.currentThread().getId(), (k) -> {
					return ByteBuffer.allocateDirect(CrailConstants.BUFFER_SIZE);
				});
				
				data.clear().limit(readRequest.length());
				try {
					int read = actionManagers.get(readRequest.getKey())
							.read(readRequest.getAddress(), data, readRequest.getChannel());
					ActiveStorageResponse.ReadResponse readResponse = new ActiveStorageResponse.ReadResponse(data, read);
					return new ActiveStorageResponse(readResponse);
				} catch (NoActionException e) {
					return new ActiveStorageResponse(ActiveStorageProtocol.REQ_READ,
							ActiveStorageProtocol.RET_NOT_CREATED);
				}
			}
			case ActiveStorageProtocol.REQ_DEL: {
				ActiveStorageRequest.DeleteRequest deleteRequest = request.getDeleteRequest();
				// LOG.info("processing active delete request, key " + deleteRequest.getKey()
				// 		+ ", address " + deleteRequest.getAddress());
				try {
					actionManagers.get(deleteRequest.getKey()).delete(deleteRequest.getAddress());
				} catch (NoActionException ignored) {
				}
				ActiveStorageResponse.DeleteResponse deleteResponse = new ActiveStorageResponse.DeleteResponse();
				return new ActiveStorageResponse(deleteResponse);
			}
			case ActiveStorageProtocol.REQ_OPEN: {
				ActiveStorageRequest.OpenRequest openRequest = request.getOpenRequest();
				// LOG.info("processing active channel open, key " + openRequest.getKey()
				// 		+ ", address " + openRequest.getAddress());
				try {
					long channel = actionManagers.get(openRequest.getKey())
							.openChannel(openRequest.getAddress());
					ActiveStorageResponse.OpenResponse openResponse = new ActiveStorageResponse.OpenResponse(channel);
					return new ActiveStorageResponse(openResponse);
				} catch (NoActionException e) {
					return new ActiveStorageResponse(ActiveStorageProtocol.REQ_OPEN,
							ActiveStorageProtocol.RET_NOT_CREATED);
				}
			}
			case ActiveStorageProtocol.REQ_CLOSE: {
				ActiveStorageRequest.CloseRequest closeRequest = request.getCloseRequest();
				// LOG.info("processing active channel close, key " + closeRequest.getKey()
				// 		+ ", address " + closeRequest.getAddress()
				// 		+ ", channel " + closeRequest.getChannel());
				try {
					if (closeRequest.isWrite()) {
						actionManagers.get(closeRequest.getKey())
								.closeWrite(closeRequest.getAddress(), closeRequest.getChannel());
					} else {
						actionManagers.get(closeRequest.getKey())
								.closeRead(closeRequest.getAddress(), closeRequest.getChannel());
					}
				} catch (NoActionException ignored) {
				} catch (InterruptedException e) {
					LOG.info("Could not close channel: " + closeRequest.getChannel());
				}
				ActiveStorageResponse.CloseResponse closeResponse = new ActiveStorageResponse.CloseResponse();
				return new ActiveStorageResponse(closeResponse);
			}
			default:
				LOG.info("processing unknown request");
				return new ActiveStorageResponse(ActiveStorageProtocol.RET_RPC_UNKNOWN);
		}
	}

	@Override
	public void addEndpoint(NaRPCServerChannel channel) {
	}

	@Override
	public void removeEndpoint(NaRPCServerChannel channel) {
	}
}
