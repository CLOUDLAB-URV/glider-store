package org.apache.crail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Future;

import org.apache.crail.core.ActiveReadableChannel;
import org.apache.crail.core.ActiveWritableChannel;

/**
 * Proxy interface to access Crail Active Objects
 */
public interface CrailObjectProxy {

	/**
	 * Instantiates an action on this object. Each object can only
	 * have one action.
	 *
	 * @param actionClass The class defining the action.
	 * @throws Exception if there is an error processing the request.
	 */
	void create(Class<? extends CrailAction> actionClass) throws Exception;

	/**
	 * Removes the action existing in this object. It does not remove
	 * the object: use the CrailStore client to remove Crail nodes.
	 *
	 * @throws Exception
	 */
	void delete() throws Exception;

	/**
	 * Obtain an InputStream to read data from this object's action.
	 * It is a buffered stream to accommodate for remote message size.
	 *
	 * @return An input stream to this object.
	 * @throws IOException if the necessary channel could not be created.
	 */
	InputStream getInputStream() throws IOException;

	/**
	 * Obtain an OutputStream to write data to this object's action.
	 * It is a buffered stream to accommodate for remote message size.
	 *
	 * @return An output stream to this object.
	 * @throws IOException if the necessary channel could not be created.
	 */
	OutputStream getOutputStream() throws IOException;

	/**
	 * Obtain a WritableChannel to send data to this object's action.
	 * Buffers can be split to accommodate for remote message size.
	 *
	 * @return A WritableChannel
	 * @throws IOException if the channel could not be created.
	 */
	ActiveWritableChannel getWritableChannel() throws IOException;

	/**
	 * Obtain a ReadableChannel to retrieve data from this object's action.
	 * Buffers can be split to accommodate for remote message size.
	 *
	 * @return A ReadableChannel
	 * @throws IOException if the channel could not be created.
	 */
	ActiveReadableChannel getReadableChannel() throws IOException;
}
