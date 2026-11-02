package iwillfall.telegram.proxy;

import java.nio.*;
import java.security.GeneralSecurityException;
import java.util.*;
import javax.crypto.Cipher;
import javax.crypto.spec.*;

public final class AesCipher {
    private final Cipher cipher;

    public AesCipher(byte[] key, byte[] iv) {
        try {
            cipher = Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new IvParameterSpec(iv));
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public synchronized byte[] update(byte[] b) {
        return b.length == 0 ? b : cipher.update(b);
    }

    public record Handshake(int dc, boolean media, int protocol, byte[] prekeyIv) {
    }

    public record Context(AesCipher cltDec, AesCipher cltEnc, AesCipher tgEnc, AesCipher tgDec) {
    }

    public static Handshake handshake(byte[] h, byte[] secret) {
        if (h.length != 64) {
            return null;
        }
        byte[] p = Utils.slice(h, 8, 56);
        byte[] plain = new AesCipher(Utils.sha(Utils.slice(p, 0, 32), secret), Utils.slice(p, 32, 48)).update(h);
        int tag = Utils.le(plain, 56);
        if (tag != Utils.ABRIDGED && tag != Utils.INTERMEDIATE && tag != Utils.SECURE) {
            return null;
        }
        short dc = ByteBuffer.wrap(plain, 60, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
        return new Handshake(Math.abs((int) dc), dc < 0, tag, p);
    }

    public static byte[] relay(int tag, int dc) {
        byte[] b;
        Set<Integer> reserved = Set.of(0x44414548, 0x54534f50, 0x20544547, Utils.INTERMEDIATE,
                Utils.SECURE, 0x02010316, 0x4954504f);
        do {
            b = Utils.random(64);
        } while ((b[0] & 255) == 239 || reserved.contains(Utils.le(b, 0)) || Utils.le(b, 4) == 0);
        byte[] tail = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putInt(tag)
                .putShort((short) dc).put(Utils.random(2)).array();
        byte[] encrypted = new AesCipher(Utils.slice(b, 8, 40), Utils.slice(b, 40, 56)).update(b);
        for (int i = 0; i < 8; i++) {
            b[56 + i] = (byte) (tail[i] ^ encrypted[56 + i] ^ b[56 + i]);
        }
        return b;
    }

    public static Context context(byte[] p, byte[] secret, byte[] relay) {
        byte[] rev = Utils.reverse(p), rrev = Utils.reverse(Utils.slice(relay, 8, 56));
        AesCipher cd = new AesCipher(Utils.sha(Utils.slice(p, 0, 32), secret), Utils.slice(p, 32, 48));
        AesCipher ce = new AesCipher(Utils.sha(Utils.slice(rev, 0, 32), secret), Utils.slice(rev, 32, 48));
        AesCipher te = new AesCipher(Utils.slice(relay, 8, 40), Utils.slice(relay, 40, 56));
        AesCipher td = new AesCipher(Utils.slice(rrev, 0, 32), Utils.slice(rrev, 32, 48));
        cd.update(new byte[64]);
        te.update(new byte[64]);
        return new Context(cd, ce, te, td);
    }
}
