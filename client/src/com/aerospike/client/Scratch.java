package com.aerospike.client;

import com.aerospike.client.Value.BytesValue;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.policy.ClientPolicy;
import gnu.crypto.hash.RipeMD160;

import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * NOTES:
 * Error -8,3,0,30000,0,2,BB9020011AC4202 127.0.0.1 3000: java.io.EOFException means
 * we've exceeded the batch size limit for keys. E.g. 4096 is right out as is 3072. 2048 is ok though.
 */
public class Scratch {

  static byte[] set = new byte[] { 89, 23, 38, -52, 76, -58, -91, 43 };
  static byte[] bin = "b".getBytes(StandardCharsets.UTF_8);

  static long totalTimes = 0;
  static long batchTimes = 0;
  static int batches = 0;
  static long maxBatchTime = 0;
  static int runs = 4;
  static int validatedKeys = 0;

  public static void main(final String[] args) {
    Mode mode = Mode.valueOf(args[0]);
    System.out.println("----------------------------------------");
    System.out.println("MODE: " + mode);
    System.out.println("----------------------------------------");
    ClientPolicy policy = new ClientPolicy();

    EventPolicy eventPolicy = new EventPolicy();
    switch (mode) {
      case NATIVE_ASYNC_NIO:
      case CUSTOM_ASYNC_NIO:
        policy.eventLoops = new NioEventLoops(eventPolicy, Runtime.getRuntime().availableProcessors());
        break;
//      case NATIVE_ASYNC_NETTY:
//      case CUSTOM_ASYNC_NETTY:
//        policy.eventLoops = new NettyEventLoops(eventPolicy, new NioEventLoopGroup(Runtime.getRuntime().availableProcessors()));
//        break;
//      case NATIVE_ASYNC_NETTY_EPOLL:
//      case CUSTOM_ASYNC_EPOLL:
//        policy.eventLoops = new NettyEventLoops(eventPolicy, new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors()));
//        break;
    }
    //final Host[] hosts = Host.parseHosts("supercell-11.yms.bf1.yahoo.com,supercell-12.yms.bf1.yahoo.com,supercell-13.yms.bf1.yahoo.com,supercell-14.yms.bf1.yahoo.com,supercell-15.yms.bf1.yahoo.com",3000);
    final Host[] hosts = Host.parseHosts("localhost",3000);
    final String namespace = "test"; //"yamas";
    AerospikeClient asClient = new AerospikeClient(policy, hosts);


    if (false) {
      // just one call.....
//      ClientPolicy policy = new ClientPolicy();
//      EventPolicy eventPolicy = new EventPolicy();
//      policy.eventLoops = new NioEventLoops(eventPolicy, Runtime.getRuntime().availableProcessors());
//      final Host[] hosts = Host.parseHosts("localhost",3000);
//      AerospikeClient asClient = new AerospikeClient(policy, hosts);
//
//      final String namespace = "test";
      final String set = "demoset";
      final String keyStr = "sampleKey28";
      final String mkey = "key1";
      final String value = "value1";
      final String defaultBin = "bin1";

      Key[] keys = new Key[3072];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = new Key(namespace, set, keyStr + 1);
      }

      if (false) {
        Record[] records = asClient.get(null, keys);
        for (int i = 0; i < keys.length; i++) {
          if (records[i] == null) {
            System.out.println(i + " NOTHING!");
          } else {
            System.out.println(records[i]);
          }
        }
      } else {
        asClient.get(null, new RecordArrayListener() {
          @Override
          public void onSuccess(Key[] keys, Record[] records) {
            for (int i = 0; i < keys.length; i++) {
              if (records[i] == null) {
                System.out.println(i + " NOTHING!");
              } else {
                System.out.println(records[i]);
              }
            }
          }

          @Override
          public void onFailure(AerospikeException exception) {
            System.out.println("FAIL!");
            exception.printStackTrace();
          }
        }, null, keys);
      }
      asClient.close();
      System.exit(0);
    }

    String setName = new String(set, StandardCharsets.US_ASCII);

    int batchSize = 2048;
    UpdatingRipeMD160 hasher = new UpdatingRipeMD160();
    Key[] keys = new Key[batchSize];
    byte[][] bkeys = new byte[batchSize][];
    for (int x = 0; x < bkeys.length; x++) {
      bkeys[x] = new byte[12];
    }

    for (int i = 0; i < runs; i++) {
      int keysRequested = 0;
      int keysRead = 0;
      validatedKeys = 0;
      try {
        FileInputStream fis = new FileInputStream("hashes.bin");

        int idx = 0;
        long overallStart = System.nanoTime();

        while (true) {
          //byte[] key = new byte[12];
          int read = fis.read(bkeys[idx]);
          if (read < 0) {
            break;
          }

          switch (mode) {
            case NATIVE_SYNC:
            case NATIVE_ASYNC_NETTY:
            case NATIVE_ASYNC_NETTY_EPOLL:
            case NATIVE_ASYNC_NIO:
              hasher.update(set, 0, set.length);
              hasher.update((byte) ParticleType.BLOB);
              hasher.update(bkeys[idx], 0, bkeys[idx].length);
              final byte[] digest = hasher.digest(); // updates in the ByteValue
              if (keys[idx] == null) {
                keys[idx] = new Key(namespace, Arrays.copyOf(digest, digest.length), setName,
                        new BytesValue(bkeys[idx]));
              } else {
                byte[] keyBytes = (byte[]) keys[idx].userKey.getObject();
                System.arraycopy(bkeys[idx], 0, keyBytes, 0, bkeys[idx].length);
                System.arraycopy(digest, 0, keys[idx].digest, 0, digest.length);
              }
              //keys[idx] = new Key("yamas", setName, bkeys[idx]);
              break;
          }
//          System.out.println("BUILT KEY " + Arrays.toString(keys[idx].digest) + " From key " +
//                  Arrays.toString(bkeys[idx]));
          idx++;

          if (idx == bkeys.length) {
            // flush
            keysRequested += idx;
            switch (mode) {
              case NATIVE_SYNC:
                keysRead += sync(asClient, keys);
                break;
              case NATIVE_ASYNC_NIO:
              case NATIVE_ASYNC_NETTY:
              case NATIVE_ASYNC_NETTY_EPOLL:
                keysRead += async(asClient, keys);
                break;
              case CUSTOM_ASYNC_NIO:
              case CUSTOM_ASYNC_NETTY:
              case CUSTOM_ASYNC_EPOLL:
                throw new UnsupportedOperationException("TODO");
                //keysRead += lowlevel(client, bkeys);
                //break;
            }
            idx = 0;
          }


          if (keysRequested >= 500000) {
            break;
          }
        }

        if (idx > 0) {
          // flush last
          keysRequested += idx;
          switch(mode) {
            case NATIVE_SYNC:
              keysRead += sync(asClient, Arrays.copyOfRange(keys, 0, idx));
              break;
            case NATIVE_ASYNC_NIO:
            case NATIVE_ASYNC_NETTY:
            case NATIVE_ASYNC_NETTY_EPOLL:
              keysRead += async(asClient, Arrays.copyOfRange(keys, 0, idx));
              break;
            case CUSTOM_ASYNC_NIO:
            case CUSTOM_ASYNC_NETTY:
            case CUSTOM_ASYNC_EPOLL:
              //keysRead += lowlevel(client, Arrays.copyOfRange(bkeys, 0, idx));
              throw new UnsupportedOperationException("TODO");
              //break;
          }
          //keysRead += lowlevel(client, Arrays.copyOfRange(bkeys, 0, idx));
          //System.out.println("BT: " + tt);
        }
        long tt = System.nanoTime() - overallStart;
        totalTimes += tt;
        fis.close();
        System.out.println("Finished a test in " + ((double) tt / 1_000_000d)
                + "ms  Keys requested: " + keysRequested
                + "  keysRead: " + keysRead
                + "  Validated: "  + validatedKeys);
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }

    System.out.println("Avg Total time: " + ((double) totalTimes / (double) runs / 1_000_000d));
    System.out.println("Avg Batch time: " + ((double) batchTimes / (double) batches / (double) runs / 1_000_000d));
    System.out.println("Batches: " + (batches / runs));
    System.out.println("Max batch time: " + ((double) maxBatchTime / 1_000_000d));

    asClient.close();
    if (policy.eventLoops != null) {
      policy.eventLoops.close();
    }
  }

  static enum Mode {
    NATIVE_SYNC,
    NATIVE_ASYNC_NIO,
    NATIVE_ASYNC_NETTY,
    NATIVE_ASYNC_NETTY_EPOLL,
    CUSTOM_ASYNC_NIO,
    CUSTOM_ASYNC_NETTY,
    CUSTOM_ASYNC_EPOLL,
  }

  static int sync(AerospikeClient client, Key[] keys) {
    long start = System.nanoTime();
    Record[] records = client.get(client.batchPolicyDefault, keys);
    long tt = System.nanoTime() - start;
    if (tt > maxBatchTime) {
      maxBatchTime = tt;
    }
    batchTimes += tt;
    batches++;
    int read = 0;
    for (int i = 0; i < records.length; i++) {
      if (records[i] != null) {
        ++read;
        byte[] kv = (byte[]) keys[i].userKey.getObject();
        byte[] r = null;
        if (records[i].bins != null) {
          r = (byte[]) records[i].getValue("b");
        } else {
          r = (byte[]) ((BufferedRecord) records[i]).getValueFromBuffer("b");
        }
        for (int x = 0; x < kv.length; x++) {
          if (r[x] != kv[x]) {
            System.out.println("FAIL VALUE VALIDATION!!!!!! \n" + Arrays.toString(kv) +"\n" +
                    Arrays.toString(r) +"\n");
            client.close();
            System.exit(1);
          }
        }
        validatedKeys++;
      }
    }
    return read;
  }

  static int async(AerospikeClient client, Key[] keys) {
    long start = System.nanoTime();
    CompletableFuture<Integer> future = new CompletableFuture<Integer>();
    RecordArrayListener listener = new RecordArrayListener() {

      @Override
      public void onSuccess(Key[] keys, Record[] records) {
        long tt = System.nanoTime() - start;
        if (tt > maxBatchTime) {
          maxBatchTime = tt;
        }
        batchTimes += tt;
        batches++;

        int validKeys = 0;
        for (int i = 0; i < records.length; i++) {
          Record record = records[i];
          if (record == null) {
            continue;
          }
          validKeys++;
          byte[] kv = (byte[]) keys[i].userKey.getObject();
          byte[] r = null;
          if (records[i].bins != null) {
            r = (byte[]) records[i].getValue("b");
          } else {
            r = (byte[]) ((BufferedRecord) records[i]).getValueFromBuffer("b");
          }
          for (int x = 0; x < kv.length; x++) {
            if (r[x] != kv[x]) {
              System.out.println("FAIL VALUE VALIDATION!!!!!! \n" + Arrays.toString(kv) +"\n" +
                      Arrays.toString(r) +"\n");
              client.close();
              System.exit(1);
            }
          }
          validatedKeys++;
        }

        future.complete(validKeys);
      }

      @Override
      public void onFailure(AerospikeException exception) {
        exception.printStackTrace();
        future.completeExceptionally(exception);
      }
    };

    client.get(null, listener, null, keys);
    try {
      return future.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    return -1;
  }

  public static class UpdatingRipeMD160 extends RipeMD160 {
    public static final int DIGEST_SIZE = 20;

    /** The digest we'll populate. */
    private final byte[] digest = new byte[DIGEST_SIZE];

    /** The padding buffer and index. */
    private final byte[] paddingBuffer = new byte[120];
    private int paddingBufferIdx = 0;

    /** The fields storing the computed digests. At the end these are
     * copied over to the digest array. */
    private final Field f0;
    private final Field f1;
    private final Field f2;
    private final Field f3;
    private final Field f4;

    /**
     * Default ctor just initializes the class.
     */
    public UpdatingRipeMD160() {
      super();
      // padding is always binary 1 followed by binary 0s
      paddingBuffer[0] = (byte) 0x80;
      try {
        // needs must...
        f0 = RipeMD160.class.getDeclaredField("h0");
        f0.setAccessible(true);
        f1 = RipeMD160.class.getDeclaredField("h1");
        f1.setAccessible(true);
        f2 = RipeMD160.class.getDeclaredField("h2");
        f2.setAccessible(true);
        f3 = RipeMD160.class.getDeclaredField("h3");
        f3.setAccessible(true);
        f4 = RipeMD160.class.getDeclaredField("h4");
        f4.setAccessible(true);
      } catch (NoSuchFieldException e) {
        throw new IllegalStateException("Unable to extract the digest " +
                "integers from gnu.crypto.hash.RipeMD160");
      }
    }

    @Override
    public byte[] digest() {
      padBuffer(); // pad remaining bytes in buffer
      update(paddingBuffer, 0, paddingBufferIdx); // last transform of a message
      computeResult(); // make a result out of context

      paddingBufferIdx = 0;
      reset(); // reset this instance for future re-use

      return digest;
    }

    /** @return The previously computed digest.
     * <b>WARNING</b> Make sure to call {@link #digest()} } before this method. */
    public byte[] getDigest() {
      return digest;
    }

    protected void computeResult() {
      try {
        int idx = 0;
        int h0 = f0.getInt(this);
        int h1 = f1.getInt(this);
        int h2 = f2.getInt(this);
        int h3 = f3.getInt(this);
        int h4 = f4.getInt(this);

        digest[idx++] = (byte) h0;
        digest[idx++] = (byte) (h0 >>> 8);
        digest[idx++] = (byte) (h0 >>> 16);
        digest[idx++] = (byte) (h0 >>> 24);
        digest[idx++] = (byte) h1;
        digest[idx++] = (byte) (h1 >>> 8);
        digest[idx++] = (byte) (h1 >>> 16);
        digest[idx++] = (byte) (h1 >>> 24);
        digest[idx++] = (byte) h2;
        digest[idx++] = (byte) (h2 >>> 8);
        digest[idx++] = (byte) (h2 >>> 16);
        digest[idx++] = (byte) (h2 >>> 24);
        digest[idx++] = (byte) h3;
        digest[idx++] = (byte) (h3 >>> 8);
        digest[idx++] = (byte) (h3 >>> 16);
        digest[idx++] = (byte) (h3 >>> 24);
        digest[idx++] = (byte) h4;
        digest[idx++] = (byte) (h4 >>> 8);
        digest[idx++] = (byte) (h4 >>> 16);
        digest[idx++] = (byte) (h4 >>> 24);
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }

    @Override
    protected byte[] padBuffer() {
      int n = (int)(count % 64);
      int padding = (n < 56) ? (56 - n) : (120 - n);
      Arrays.fill(paddingBuffer, 1, padding, (byte) 0);

      // save number of bits, casting the long to an array of 8 bytes
      long bits = count << 3;
      paddingBuffer[padding++] = (byte) bits;
      paddingBuffer[padding++] = (byte)(bits >>>  8);
      paddingBuffer[padding++] = (byte)(bits >>> 16);
      paddingBuffer[padding++] = (byte)(bits >>> 24);
      paddingBuffer[padding++] = (byte)(bits >>> 32);
      paddingBuffer[padding++] = (byte)(bits >>> 40);
      paddingBuffer[padding++] = (byte)(bits >>> 48);
      paddingBuffer[padding  ] = (byte)(bits >>> 56);

      paddingBufferIdx = padding + 1;
      return paddingBuffer;
    }
  }
}
