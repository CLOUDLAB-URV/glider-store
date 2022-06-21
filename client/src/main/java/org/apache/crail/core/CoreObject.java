package org.apache.crail.core;

import org.apache.crail.CrailObject;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.metadata.FileInfo;

/**
 * CrailNode holding a computational action.
 */
public class CoreObject extends CoreNode implements CrailObject {

	public CoreObject(CoreDataStore fs, FileInfo fileInfo, String path) {
		super(fs, fileInfo, path);
	}

	@Override
	public CrailObject asObject() throws Exception {
		if (!getType().isObject()) {
			throw new Exception("object type mismatch, type " + getType());
		}
		return this;
	}

	@Override
	public long getToken() {
		return fileInfo.getToken();
	}

	@Override
	public CrailObjectProxy getProxy() throws Exception {
		return new CoreObjectProxy(this);
	}
}
