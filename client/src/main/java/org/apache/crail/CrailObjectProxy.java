package org.apache.crail;

import java.util.concurrent.Future;

public interface CrailObjectProxy {
	void write(byte[] bytes) throws Exception;
	Future<CrailResult> write(CrailBuffer dataBuf) throws Exception;
	int read(byte[] bytes) throws Exception;
	Future<CrailResult> read(CrailBuffer dataBuf) throws Exception;
	void create(Class<? extends CrailAction> actionClass) throws Exception;
	void delete() throws Exception;
}
