package org.apache.crail.storage.active;

import org.apache.crail.storage.tcp.TcpStorageServer;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class ActiveStorageServer extends TcpStorageServer {
    private static final Logger LOG = CrailUtils.getLogger();

    @Override
    public void run() {
        LOG.info("running Active storage server intercepting");
        super.run();
    }
}
