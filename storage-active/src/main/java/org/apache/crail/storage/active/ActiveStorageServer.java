package org.apache.crail.storage.active;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
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
import org.apache.crail.CrailAction;
import org.apache.crail.CrailObject;
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
	private ConcurrentHashMap<Integer, ConcurrentHashMap<Long, CrailAction>> actions;

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
				TcpStorageConstants.STORAGE_TCP_CORES);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		this.address = StorageUtils.getDataNodeAddress(TcpStorageConstants.STORAGE_TCP_INTERFACE,
				TcpStorageConstants.STORAGE_TCP_PORT);
		serverEndpoint.bind(address);
		this.alive = false;
		this.regions = TcpStorageConstants.STORAGE_TCP_STORAGE_LIMIT / TcpStorageConstants.STORAGE_TCP_ALLOCATION_SIZE;
		this.keys = 0;
		this.actions = new ConcurrentHashMap<>();

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
			int fileId = (int) keys++;
			actions.put(fileId, new ConcurrentHashMap<>());
			// Object capacity per region is the number of blocks it fits
			resource = StorageResource.createResource(0,
					(int) TcpStorageConstants.STORAGE_TCP_ALLOCATION_SIZE,
					fileId);
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
				LOG.info("processing active create request, key " + createRequest.getKey()
						+ ", address " + createRequest.getAddress() + ", action class " + createRequest.getName());
				CrailAction action = actions.get(createRequest.getKey())
						.computeIfAbsent(createRequest.getAddress(), key -> {
							try {
								Class<? extends CrailAction> actionClass =
										Class.forName(createRequest.getName()).asSubclass(CrailAction.class);
								CrailObject node = fs.lookup(createRequest.getPath()).get().asObject();
								CrailAction a = actionClass.newInstance();
								Method method = CrailAction.class.getDeclaredMethod("init", CrailObject.class);
								method.setAccessible(true);
								method.invoke(a, node);
								return a;
							} catch (ClassNotFoundException | ClassCastException
									| InstantiationException | IllegalAccessException e) {
								// Error in dynamic class load or instantiation
								LOG.info("Class not found: '" + createRequest.getName() + "' -> " + e);
								return null;
							} catch (Exception e) {
								// cloud not complete the lookup
								LOG.info("Object creating is not in namenode.");
								return null;
							}
						});
				if (action == null) {
					return new ActiveStorageResponse(ActiveStorageProtocol.REQ_CREATE,
							ActiveStorageProtocol.RET_NOT_CREATED);
				} else {
					ActiveStorageResponse.CreateResponse createResponse = new ActiveStorageResponse.CreateResponse();
					return new ActiveStorageResponse(createResponse);
				}
			}
			case ActiveStorageProtocol.REQ_WRITE: {
				ActiveStorageRequest.WriteRequest writeRequest = request.getWriteRequest();
				LOG.info("processing active write request, key " + writeRequest.getKey()
						+ ", address " + writeRequest.getAddress() + ", length " + writeRequest.length()
						+ ", remaining " + writeRequest.getBuffer().remaining());

				CrailAction action = actions.get(writeRequest.getKey()).get(writeRequest.getAddress());
				if (action == null) {
					return new ActiveStorageResponse(ActiveStorageProtocol.REQ_WRITE,
							ActiveStorageProtocol.RET_NOT_CREATED);
				}
				int written = action.onWrite(writeRequest.getBuffer().duplicate());
				ActiveStorageResponse.WriteResponse writeResponse =
						new ActiveStorageResponse.WriteResponse(written);
				return new ActiveStorageResponse(writeResponse);
			}
			case ActiveStorageProtocol.REQ_READ: {
				ActiveStorageRequest.ReadRequest readRequest = request.getReadRequest();
				LOG.info("processing active read request, address " + readRequest.getAddress()
						+ ", length " + readRequest.length());

				CrailAction action = actions.get(readRequest.getKey()).get(readRequest.getAddress());
				if (action == null) {
					return new ActiveStorageResponse(ActiveStorageProtocol.REQ_READ,
							ActiveStorageProtocol.RET_NOT_CREATED);
				}
				ByteBuffer data = ByteBuffer.allocateDirect(readRequest.length());
				action.onRead(data);
				data.clear();
				ActiveStorageResponse.ReadResponse readResponse = new ActiveStorageResponse.ReadResponse(data);
				return new ActiveStorageResponse(readResponse);
			}
			case ActiveStorageProtocol.REQ_DEL: {
				ActiveStorageRequest.DeleteRequest deleteRequest = request.getDeleteRequest();
				LOG.info("processing active delete request, key " + deleteRequest.getKey()
						+ ", address " + deleteRequest.getAddress());
				CrailAction action = actions.get(deleteRequest.getKey()).get(deleteRequest.getAddress());
				if (action != null) {
					action.onDelete();
					actions.get(deleteRequest.getKey()).remove(deleteRequest.getAddress());
				}
				ActiveStorageResponse.DeleteResponse deleteResponse = new ActiveStorageResponse.DeleteResponse();
				return new ActiveStorageResponse(deleteResponse);
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
