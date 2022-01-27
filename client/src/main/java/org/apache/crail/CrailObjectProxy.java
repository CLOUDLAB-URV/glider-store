package org.apache.crail;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Future;

/**
 * Proxy interface to access Crail Active Objects
 */
public interface CrailObjectProxy {
	/**
	 * Send a byte array directly to this object's action. The array is
	 * wrapped in a CrailBuffer.
	 *
	 * @param bytes
	 * @return
	 * @throws Exception
	 */
	int write(byte[] bytes) throws Exception;

	/**
	 * Sent a byte buffer directly to this object's action.
	 *
	 * @param dataBuf
	 * @return
	 * @throws Exception
	 */
	Future<CrailResult> write(CrailBuffer dataBuf) throws Exception;

	/**
	 * Retrieve a byte array directly from this object's action. The
	 * given array will be populated with the data returned from the
	 * action.
	 *
	 * @param bytes
	 * @return
	 * @throws Exception
	 */
	int read(byte[] bytes) throws Exception;

	/**
	 * Retrieve a byte buffer directly from this object's action. The
	 * given buffer will be populated with the data returned from the
	 * action.
	 *
	 * @param dataBuf
	 * @return
	 * @throws Exception
	 */
	Future<CrailResult> read(CrailBuffer dataBuf) throws Exception;

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
	 * @return
	 */
	InputStream getInputStream();

	/**
	 * Obtain an OutputStream to write data to this object's action.
	 * It is a buffered stream to accommodate for remote message size.
	 *
	 * @return
	 */
	OutputStream getOutputStream();
}
