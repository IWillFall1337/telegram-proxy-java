package iwillfall.telegram.proxy;

import java.util.concurrent.CompletableFuture;

public interface ByteStream extends AutoCloseable {
    CompletableFuture<byte[]> read(int count);
    CompletableFuture<Void> write(byte[] bytes);
    void close();
}
