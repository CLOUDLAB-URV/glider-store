package org.apache.crail.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.crail.CrailResult;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.storage.ActiveEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class ActiveAsyncChannel extends ActiveChannel implements AsynchronousByteChannel {
    private static final Logger LOG = CrailUtils.getLogger();
    private boolean open;
    private long totalBytes = 0;
    private String mode;

    ActiveAsyncChannel(CoreObject object, ActiveEndpoint endpoint, BlockInfo block, String mode) throws IOException {
        super(object, endpoint, block);
        this.open = true;
        if (mode == "r" || mode == "w") {
            this.mode = mode;
        } else {
            throw new IllegalStateException("AsyncChannel model con only be r or w.");
        }
        if (CrailConstants.DEBUG) {
            LOG.info("ActiveAsyncChannel, open, path " + object.getPath());
        }
    }

    @Override
    public void close() throws IOException {
        if (!open) {
            return;
        }
        try {
            if (mode == "r") {
                endpoint.closeRead(block, position, channelId).get();
            } else if (mode == "w") {
                endpoint.closeWrite(block, position, channelId).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("ActiveAsyncChannel:close - Future not completed.", e);
        }
        open = false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public Future<Integer> read(ByteBuffer dst) {
        if (!open) {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            future.completeExceptionally(new ClosedChannelException());
            return future;
        }
        if (dst.remaining() <= 0) {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            future.complete(0);
            return future;
        }
        if (mode == "r") {
            try {
                CoreObjectOperation operation = dataOperation(dst);
                return new ChannelFuture(operation);
            } catch (IOException e) {
                CompletableFuture<Integer> future = new CompletableFuture<>();
                future.completeExceptionally(e);
                return future;
            }
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        // TODO Auto-generated method stub

    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        if (!open) {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            future.completeExceptionally(new ClosedChannelException());
            return future;
        }
        if (src.remaining() <= 0) {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            future.complete(0);
            return future;
        }
        if (mode == "w") {
            try {
                CoreObjectOperation operation = dataOperation(src);
                return new ChannelFuture(operation);
            } catch (IOException e) {
                CompletableFuture<Integer> future = new CompletableFuture<>();
                future.completeExceptionally(e);
                return future;
            }
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public <A> void write(ByteBuffer src, A attachment, CompletionHandler<Integer, ? super A> handler) {
        // TODO Auto-generated method stub

    }

    @Override
    StorageFuture trigger(CoreSubOperation operation, ByteBuffer buffer) throws IOException {
        if (mode == "r") {
            return endpoint.readStream(buffer, block, operation.getFileOffset(), channelId);
        } else if (mode == "w") {
            return endpoint.writeStream(buffer, block, operation.getFileOffset(), channelId);
        } else {
            throw new IllegalStateException();
        }
    }

    private class ChannelFuture implements Future<Integer> {
        CoreObjectOperation operation;

        ChannelFuture(CoreObjectOperation operation) {
            this.operation = operation;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return operation.cancel(mayInterruptIfRunning);
        }

        @Override
        public Integer get() throws InterruptedException, ExecutionException {
            CrailResult result = operation.get();
            // If multi-operation got end of stream, the aggregation could be < -1
            if (result.getLen() < 0)
                return -1;
            return (int) result.getLen();
        }

        @Override
        public Integer get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            CrailResult result = operation.get(timeout, unit);
            // If multi-operation got end of stream, the aggregation could be < -1
            if (result.getLen() < 0)
                return -1;
            return (int) result.getLen();
        }

        @Override
        public boolean isCancelled() {
            return operation.isCancelled();
        }

        @Override
        public boolean isDone() {
            return operation.isDone();
        }
    }

}
