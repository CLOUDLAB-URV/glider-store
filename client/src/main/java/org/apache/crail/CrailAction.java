package org.apache.crail;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Base class of all Crail Actions.
 * <p>
 * User classes extend this one to define the operations that should
 * run when an active object is accessed for read or write. The user
 * can also define logic to run when the action is created or
 * deleted.
 */
public abstract class CrailAction {
	protected CrailStore fs;
	protected CrailObject self;

	private void init(CrailObject node) {
		this.self = node;
		this.fs = node.getFileSystem();
		onCreate();
	}

	/**
	 * This runs when the Action instance is created. That is, when
	 * the user calls <code>create</code> on an active object proxy
	 * and provides an Action class to instantiate.
	 */
	public void onCreate() {}

	/**
	 * This runs when the user calls <code>read</code> directly to
	 * the active object proxy by providing a specific byte array
	 * or ByteBuffer.
	 * <p>
	 * The action should populate the given ByteBuffer with the data
	 * that is to be returned to the client. The size of the buffer
	 * must fit into an RPC message.
	 *
	 * @param buffer buffer that will be directly sent to the client.
	 */
	@Deprecated
	public void onRead(ByteBuffer buffer) {}

	/**
	 * This runs when the user calls <code>write</code> directly to
	 * the active object proxy by providing a specific byte array
	 * or ByteBuffer.
	 * <p>
	 * The action should obtain the data sent by the client from the
	 * given ByteBuffer. The size of the buffer must fit into an RPC
	 * message.
	 *
	 * @param buffer buffer that was sent from the client.
	 * @return the number of bytes written.
	 */
	@Deprecated
	public int onWrite(ByteBuffer buffer) {return 0;}

	/**
	 * This runs when the user reads from a stream obtained from an
	 * active object proxy through <code>getInputStream</code>.
	 * <p>
	 * The action should provide the data to be sent to the client by
	 * writing to the given OutputStream.
	 *
	 * @param channel Data channel to respond to a client read.
	 */
	public void onReadStream(WritableByteChannel channel) {}

	/**
	 * This runs when the user writes to a stream obtained from an
	 * active object proxy through <code>getOutputStream</code>.
	 * <p>
	 * The action should obtain the data sent from the client by
	 * reading from the given InputStream.
	 *
	 * @param channel Data channel to receive a client write.
	 */
	public void onWriteStream(ReadableByteChannel channel) {}

	/**
	 * This runs when the Action instance is deleted. That is, when
	 * the user calls <code>delete</code> on an active object proxy.
	 */
	public void onDelete() {}
}
