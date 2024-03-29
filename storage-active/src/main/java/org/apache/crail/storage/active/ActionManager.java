package org.apache.crail.storage.active;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.crail.CrailAction;
import org.apache.crail.CrailObject;
import org.apache.crail.CrailStore;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

/**
 * Manages all actions running in a server region.
 */
public class ActionManager {
	private static final Logger LOG = CrailUtils.getLogger();

	private final CrailStore fs;
	private final ConcurrentHashMap<Long, CrailAction> actions;
	private final ConcurrentHashMap<Long, Lock> actionLocks;
	private final ConcurrentHashMap<Long, BlockingQueue<OperationSlice>> channels;
	private final ExecutorService actionExecutorService;
	private final AtomicLong idGen;

	public ActionManager(CrailStore fs) {
		this.fs = fs;
		actions = new ConcurrentHashMap<>();
		actionLocks = new ConcurrentHashMap<>();
		channels = new ConcurrentHashMap<>();
		actionExecutorService = Executors.newCachedThreadPool();
		idGen = new AtomicLong(0);
	}

	/**
	 * Instantiate a new action in this manager.
	 * <p>
	 * If an action with this ID is already present, this does nothing.
	 * To update an action implementation, first it should be deleted
	 * with {@link ActionManager#delete(long)}.
	 *
	 * @param actionId        Unique identifier for the action.
	 * @param actionClassName Nome of the Action implementation class.
	 *                        Should extend {@link CrailAction}.
	 * @param crailPath       The path to this action in the global Crail FS.
	 * @throws NoActionException If the action could not be created.
	 */
	public void create(long actionId, String actionClassName, String crailPath, boolean interleaving)
			throws NoActionException {
		CrailAction action = actions.computeIfAbsent(actionId, key -> {
			try {
				Class<? extends CrailAction> actionClass = Class.forName(actionClassName).asSubclass(CrailAction.class);
				CrailObject node = fs.lookup(crailPath).get().asObject();
				CrailAction a = actionClass.newInstance();
				Method method = CrailAction.class.getDeclaredMethod("init", CrailObject.class, boolean.class);
				method.setAccessible(true);
				method.invoke(a, node, interleaving);
				actionLocks.put(actionId, new ReentrantLock());
				return a;
			} catch (ClassNotFoundException | ClassCastException
							 | InstantiationException | IllegalAccessException e) {
				// Error in dynamic class load or instantiation
				LOG.info(String.format("Class not found: '%s' -> %s", actionClassName, e));
				return null;
			} catch (InterruptedException e) {
				// interrupted on lookup
				LOG.info("Interrupted call to crail lookup.");
				e.printStackTrace();
				Thread.currentThread().interrupt();
				return null;
			} catch (Exception e) {
				// cloud not complete the lookup
				LOG.info("Object creating is not in namenode.");
				e.printStackTrace();
				return null;
			}
		});
		// If action is null -> it could not be created
		if (action == null) {
			throw new NoActionException();
		}
		// If action was already created, ignore (to update action impl, first delete)
	}

	/**
	 * Remove an action from this manager.
	 * <p>
	 * If the action is running a data operation, this will wait until
	 * the operations are finished. Beware of deadlocks.
	 *
	 * @param actionId Unique identifier for the action.
	 * @throws NoActionException if the action does not exist in this manager.
	 */
	public void delete(long actionId) throws NoActionException {
		Lock actionLock = actionLocks.get(actionId);
		if (actionLock == null) {
			throw new NoActionException();
		}
		actionLock.lock();  // Wait any in-progress op before deleting
		try {
			CrailAction action = actions.get(actionId);
			action.onDelete();
			actions.remove(actionId);
			actionLocks.remove(actionId);
		} finally {
			actionLock.unlock();
		}
	}

	/**
	 * Queue a read operation slice to an action on the specific channel.
	 * <p>
	 * This creates an operation slice with the given buffer and queues
	 * it to the specified channel (may block). If this slice is the
	 * first sent to the channel, it will also create that channel and
	 * trigger the execution of the action's onReadStream logic. If the
	 * channel is closed, the slice is not queued and this immediately
	 * returns end-of-stream.
	 * <p>
	 * If {@code channelId} is -1, this is treated as a direct read.
	 *
	 * @param actionId  Unique identifier for the action.
	 * @param buffer    Buffer to populate with the read operation.
	 * @param channelId Unique identifier for the channel.
	 * @return The number of bytes read from the channel, possibly 0.
	 * -1 if the channel has reached end-of-stream.
	 * @throws NoActionException if the action is not in this manager.
	 */
	public int read(long actionId, ByteBuffer buffer, long channelId) throws NoActionException {
		final CrailAction action = actions.get(actionId);
		if (action == null) {
			throw new NoActionException();
		}
		BlockingQueue<OperationSlice> channel;
		synchronized (channels) {
			channel = channels.computeIfAbsent(channelId, k -> {
				// When channel is new
				BlockingQueue<OperationSlice> c = new ArrayBlockingQueue<>(ActiveStorageConstants.STORAGE_ACTIVE_CHANNEL_SIZE);
				// submit the read task to generate data from action
				actionExecutorService.submit(new OnReadOperation(action, c, actionLocks.get(actionId)));
				return c;
			});
		}
		if (!channel.isEmpty() && channel.peek().getSlice() == null) {
			// end-of-stream -> buffer isn't affected
			return -1;
		}
		// queue this buffer request's buffer as an operation slice for the task to fill
		OperationSlice op = new OperationSlice(buffer, false);
		try {
			channel.put(op);
		} catch (InterruptedException e) {
			// This means the thread is forced to stop (the operation was never queued in the channel)
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}
		// wait until the buffer is filled (could be partial if end-of-stream)
		try {
			op.waitCompleted();
			// LOG.info("active read completed, action " + actionId + ", channel " + channelId);
		} catch (InterruptedException e) {
			// This means the thread is forced to stop
			// (the operation was queued in the channel but never taken or partially filled)
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}
		// return actual bytes read (should always be >= 0)
		return op.getBytesProcessed();
	}

	/**
	 * Queue a write operation slice to an action on the specific channel.
	 * <p>
	 * This creates an operation slice with the given buffer and queues
	 * it to the specified channel (may block). If this slice is the
	 * first sent to the channel, it will also create that channel and
	 * trigger the execution of the action's onWriteStream logic.
	 * <p>
	 * If {@code channelId} is -1, this is treated as a direct write.
	 *
	 * @param actionId  Unique identifier for the action.
	 * @param buffer    Buffer to consume with the write operation.
	 * @param channelId Unique identifier for the channel.
	 * @return The number of bytes written to the channel, possibly 0.
	 * @throws NoActionException if the action is not in this manager.
	 */
	public int write(long actionId, ByteBuffer buffer, long channelId)
			throws NoActionException {
		final CrailAction action = actions.get(actionId);
		if (action == null) {
			throw new NoActionException();
		}
		BlockingQueue<OperationSlice> channel;
		synchronized (channels) {
			channel = channels.computeIfAbsent(channelId, k -> {
				// channel is new
				BlockingQueue<OperationSlice> c = new ArrayBlockingQueue<>(ActiveStorageConstants.STORAGE_ACTIVE_CHANNEL_SIZE);
				actionExecutorService.submit(new OnWriteOperation(action, c, actionLocks.get(actionId)));
				return c;
			});
		}
		OperationSlice op = new OperationSlice(buffer, false);
		try {
			channel.put(op);
			// LOG.info("active write queued, action " + actionId + ", channel " + channelId);
		} catch (InterruptedException e) {
			// This means the thread is forced to stop (the operation was never queued in the channel)
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}
		// wait until the buffer is processed
		try {
			op.waitCompleted();
			// Waiting on writes is not necessary but allows to reuse the same buffer for all
			// operations from that network thread. This could stale network threads in deadlocks
		} catch (InterruptedException e) {
			// This means the thread is forced to stop
			// (the operation was queued in the channel but never taken or partially filled)
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}
		// return actual bytes written (should always be >= 0)
		return op.getBytesProcessed();
	}

	/**
	 * Notify the end of a write channel. Wait until the action has
	 * processed all previous slices.
	 *
	 * @param actionId  Unique identifier for the action.
	 * @param channelId Unique identifier for the channel.
	 * @throws NoActionException    if the action is not in this manager.
	 * @throws InterruptedException if thread was interrupted while waiting.
	 *                              The channel did not close.
	 */
	public void closeWrite(long actionId, long channelId)
			throws NoActionException, InterruptedException {
		CrailAction action = actions.get(actionId);
		if (action == null) {
			throw new NoActionException();
		}
		BlockingQueue<OperationSlice> channel = channels.get(channelId);
		if (channel != null) {
			channel.put(new OperationSlice());
			channels.remove(channelId);
		}
	}

	/**
	 * Notify the end of a read channel. Wait until the action has
	 * processed all previous slices. This represents an early
	 * client channel close.
	 *
	 * @param actionId  Unique identifier for the action.
	 * @param channelId Unique identifier for the channel.
	 * @throws NoActionException    if the action is not in this manager.
	 * @throws InterruptedException if thread was interrupted while waiting.
	 *                              The channel did not close.
	 */
	public void closeRead(long actionId, long channelId)
			throws NoActionException, InterruptedException {
		CrailAction action = actions.get(actionId);
		if (action == null) {
			throw new NoActionException();
		}
		BlockingQueue<OperationSlice> channel = channels.get(channelId);
		if (channel != null) {
			if (channel.isEmpty() || channel.peek().getSlice() != null) {
				// stream is still open -> send token to close early
				channel.put(new OperationSlice());
			} // else: stream is already closed
			channels.remove(channelId);
		}  // else: channel does not exist or already closed -> ignore
	}

	/**
	 * Close this action manager. This closes all active channels
	 * (waiting for remaining operations), deletes all actions,
	 * and stops the action executor service.
	 */
	public void close() {
		// TODO:
		// 	close all active channels and operations
		// 	delete all actions
		// interrupt waiting threads (actions) and deal with early closes
		actionExecutorService.shutdown();
		try {
			if (!actionExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
				actionExecutorService.shutdownNow();
				if (!actionExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
					LOG.info("Action manager executor did not terminate.");
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Generate the next chanel identifier.
	 *
	 * @param actionId Action the new channel will be linked to.
	 * @return The identifier for the new channel.
	 * @throws NoActionException if the action is not in this manager.
	 */
	public long openChannel(long actionId) throws NoActionException {
		CrailAction action = actions.get(actionId);
		if (action == null) {
			throw new NoActionException();
		}
		return idGen.incrementAndGet();
	}

	/**
	 * Representation of the execution of an onWriteStream operation on an action.
	 */
	private static class OnWriteOperation implements Runnable {
		private final CrailAction action;
		private final BlockingQueue<OperationSlice> channel;
		private final Lock lock;

		OnWriteOperation(CrailAction action, BlockingQueue<OperationSlice> channel, Lock lock) {
			this.action = action;
			this.channel = channel;
			this.lock = lock;
		}

		@Override
		public void run() {
			lock.lock();
			try (
					OnWriteChannel owc = action.isInterleaving()
							? new OnWriteChannel(channel, lock)
							: new OnWriteChannel(channel)
			) {
				action.onWrite(owc);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				// Show any exception occurred within the action code.
				e.printStackTrace();
				throw e;
			} finally {
				lock.unlock();
			}
		}
	}

	/**
	 * Representation of the execution of an onReadStream operation on an action.
	 */
	private static class OnReadOperation implements Runnable {
		private final CrailAction action;
		private final BlockingQueue<OperationSlice> channel;
		private final Lock lock;

		OnReadOperation(CrailAction action, BlockingQueue<OperationSlice> channel, Lock lock) {
			this.action = action;
			this.channel = channel;
			this.lock = lock;
		}

		@Override
		public void run() {
			lock.lock();
			try (
					OnReadChannel orc = action.isInterleaving()
							? new OnReadChannel(channel, lock)
							: new OnReadChannel(channel)
			) {
				action.onRead(orc);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				// Show any exception occurred within the action code.
				e.printStackTrace();
				throw e;
			} finally {
				lock.unlock();
			}
		}
	}

}
