package iwillfall.telegram.proxy;

import java.io.*;
import java.nio.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;

public final class FakeTlsHandler implements ByteStream {
    private final NettyStream raw;
    private byte[] buffered = new byte[0];

    public FakeTlsHandler(NettyStream raw) {
        this.raw = raw;
    }

    public static byte[] verify(byte[] hello, byte[] secret) {
        if (hello.length < 43 || hello[0] != 22 || hello[5] != 1) {
            return null;
        }
        byte[] random = Utils.slice(hello, 11, 43), zero = hello.clone();
        Arrays.fill(zero, 11, 43, (byte) 0);
        byte[] expected = Utils.hmac(secret, zero);
        if (!MessageDigest.isEqual(Utils.slice(expected, 0, 28), Utils.slice(random, 0, 28))) {
            return null;
        }
        byte[] ts = new byte[4];
        for (int i = 0; i < 4; i++) {
            ts[i] = (byte) (random[28 + i] ^ expected[28 + i]);
        }
        if (Math.abs(System.currentTimeMillis() / 1000 - Integer.toUnsignedLong(Utils.le(ts, 0))) > 120) {
            return null;
        }
        return random;
    }

    public static byte[] serverHello(byte[] hello, byte[] secret, byte[] clientRandom) {
        byte[] sh = Utils.concat(HexFormat.of().parseHex("160303007a020000760303"), new byte[32], new byte[]{32},
                hello.length >= 76 && hello[43] == 32 ? Utils.slice(hello, 44, 76) : new byte[32],
                HexFormat.of().parseHex("130100002e00330024001d0020"), Utils.random(32),
                HexFormat.of().parseHex("002b00020304"));
        byte[] noise = Utils.random(1900 + Utils.RANDOM.nextInt(201));
        byte[] response = Utils.concat(sh, HexFormat.of().parseHex("140303000101"), record(noise));
        byte[] mac = Utils.hmac(secret, Utils.concat(clientRandom, response));
        System.arraycopy(mac, 0, response, 11, 32);
        return response;
    }

    public static byte[] record(byte[] data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int o = 0; o < data.length; o += 16384) {
            int n = Math.min(16384, data.length - o);
            out.writeBytes(new byte[]{23, 3, 3, (byte) (n >>> 8), (byte) n});
            out.write(data, o, n);
        }
        return out.toByteArray();
    }

    private CompletableFuture<byte[]> payload() {
        return raw.read(5).thenCompose(h -> {
            int n = ((h[3] & 255) << 8) | (h[4] & 255);
            CompletableFuture<byte[]> f = n == 0 ? CompletableFuture.completedFuture(new byte[0]) : raw.read(n);
            if (h[0] == 20) {
                return f.thenCompose(v -> payload());
            }
            if (h[0] != 23) {
                return Utils.failed(new EOFException("Not TLS application data"));
            }
            return n == 0 ? f.thenCompose(v -> payload()) : f;
        });
    }

    public CompletableFuture<byte[]> read(int n) {
        if (n == 0 && buffered.length > 0) {
            byte[] b = buffered;
            buffered = new byte[0];
            return CompletableFuture.completedFuture(b);
        }
        if (n > 0 && buffered.length >= n) {
            byte[] b = Utils.slice(buffered, 0, n);
            buffered = Utils.slice(buffered, n, buffered.length);
            return CompletableFuture.completedFuture(b);
        }
        return payload().thenCompose(b -> {
            if (n == 0) {
                return CompletableFuture.completedFuture(b);
            }
            buffered = Utils.concat(buffered, b);
            return read(n);
        });
    }

    public CompletableFuture<Void> write(byte[] data) {
        return raw.write(record(data));
    }

    public void close() {
        raw.close();
    }
}
