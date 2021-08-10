package com.aerospike.client;

import com.aerospike.client.Operation.Type;
import com.aerospike.client.Value.BytesValue;
import com.aerospike.client.Value.IntegerValue;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.lua.LuaBytes;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;
import com.aerospike.client.util.ThreadLocalData;
import org.luaj.vm2.LuaValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class JMHBenchmarks {

  public static void main(String[] args) {
    Context context = new Context();
    context.modeString = "NATIVE_ASYNC_NETTY";
    context.setup();
    //customAsyncBatch(null, context);
    //writeNative(new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous."), context);
    //writeReflect(null, context);

    batchMapRead(null, context);
    //oneOp(null, context);
    context.teardown();
  }

  @State(Scope.Benchmark)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class Context {
    public AerospikeClient asClient;
    //public List<byte[]> hashes;
    public Random rnd = new Random(System.currentTimeMillis());
    public byte[] hashes = new byte[8_000_000 * 12];
    public int hashWriteIdx = 0;
    public int batchSize = 2048;
    public int keysToRead = 1024 * 16;
    public Key[] keys;
    public String namespace = "test";
    public byte[] set = new byte[] { 89, 23, 38, -52, 76, -58, -91, 43 };
    public String setString = new String(set);
    public String bin = "b";
    public byte[] binBytes = bin.getBytes(StandardCharsets.UTF_8);
    public int mapEntries = 3;
    public byte[][] mapValues;

    @Param({/*"NATIVE_SYNC", "NATIVE_ASYNC_NIO", */"NATIVE_ASYNC_NETTY"})
    public String modeString;

    @Setup(Level.Trial)
    public void setup() {
      ClientPolicy policy = new ClientPolicy();

//      ASMode mode = ASMode.valueOf(modeString);
//      EventPolicy eventPolicy = new EventPolicy();
//      switch (mode) {
//        case NATIVE_ASYNC_NIO:
//        case CUSTOM_ASYNC_NIO:
//          loops = new NioEventLoops(eventPolicy, Runtime.getRuntime().availableProcessors());
//          break;
//        case NATIVE_ASYNC_NETTY:
//        case CUSTOM_ASYNC_NETTY:
//          loops = new NettyEventLoops(eventPolicy, new KQueueEventLoopGroup(Runtime.getRuntime().availableProcessors()));
//          break;
////      case NATIVE_ASYNC_NETTY_EPOLL:
////      case CUSTOM_ASYNC_EPOLL:
////        policy.eventLoops = new NettyEventLoops(eventPolicy, new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors()));
////        break;
//      }
      final Host[] hosts = Host.parseHosts("localhost", 3000);
      final String namespace = "test"; //"yamas";
      //policy.eventLoops = loops;
      asClient = new AerospikeClient(policy, hosts);

      try {
        FileInputStream fis = new FileInputStream("hashes.bin");
        byte[] hash = new byte[12];
        while (true) {
          int read = fis.read(hash);
          if (read < 0) {
            break;
          }
          System.arraycopy(hash, 0, hashes, hashWriteIdx, 12);
          hashWriteIdx += 12;
          //hashes.add(hash);
          //hash = new byte[12];

          // TMEP LIMIT
          if (keysToRead > 0 && hashWriteIdx / 12 > keysToRead) {
            break;
          }
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

      //hashes = new byte[hashList.size()][];
      //hashList.toArray(hashes);
//      keys = new Key[batchSize];
//      for (int i = 0; i < keys.length; i++) {
//        keys[i] = new Key(namespace, new byte[20], setString, new BytesValue(new byte[12]));
//      }
//      hasher = new UpdatingRipeMD160();
//      bufferedRecords = new BufferedRecord[batchSize];
//      for (int i = 0; i < bufferedRecords.length; i++) {
//        bufferedRecords[i] = new BufferedRecord(null, 0, 0);
//      }
      mapValues = new byte[mapEntries][];
      for (int i = 0; i < mapEntries; i++) {
        mapValues[i] = new byte[2048];
        for (int x = 0; x < mapValues[i].length; x++) {
          mapValues[i][x] = (byte)rnd.nextInt(256);
        }
      }
    }

    @TearDown
    public void teardown() {
      asClient.close();
//      if (loops != null) {
//        loops.close();
//      }

      //System.out.println("NETTY STATS: " + PooledByteBufAllocator.DEFAULT.metric().directArenas());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public static void writeNative(Blackhole blackhole, Context context) {
    MapPolicy mp = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
    for (int i = 0; i < context.hashWriteIdx; i += 12 ) {
      for (int x = 0; x < context.mapEntries; x++) {
        Operation op = MapOperation.put(mp, context.bin,
                new IntegerValue(x),
                new BytesValue(context.mapValues[x]));
        Record r = context.asClient.operate(null,
                new Key(context.namespace, context.setString,
                        Arrays.copyOfRange(context.hashes, i, i + 12)), op);
        //blackhole.consume(r.toString());
        //System.out.println("Wrote " + (i / 12) + " " + x);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public static void writeReflect(Blackhole blackhole, Context context) {
    MapPolicy mp = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
    ReflectedMapOp op = new ReflectedMapOp(mp, context.bin);
    BV bv = new BV();

    bv.bytes = new byte[12];
    bv.len = 12;
    Key key = new Key(context.namespace, context.setString, bv);

    for (int i = 0; i < context.hashWriteIdx; i += 12 ) {
      for (int x = 0; x < context.mapEntries; x++) {
        op.reset(x, context.mapValues[x], 0, context.mapValues[x].length);
        System.arraycopy(context.hashes, i, bv.bytes, 0, 12);

        // GARBAGE!
        //byte[] digest = Crypto.computeDigest(context.setString, bv);
        Buffer.longToBytes(i, key.digest, 0);
        //System.arraycopy(digest, 0, key.digest, 0, digest.length);
        Record r = context.asClient.operate(null, key, op.op);
        //System.out.println("Wrote " + (i / 12) + " " + x + " => " + r);
      }
    }
  }

  static class ReflectedMapOp {
    MapPolicy policy;
    BV val = new BV();
    Packer packer = new Packer();

    Field packerOffset;
    Field packerBuffer;

    Operation op;
    ReflectedMapOp(MapPolicy mp, String bin) {
      policy = mp;
      op = new Operation(Type.MAP_MODIFY, bin, val);

      try {
        packerOffset = Packer.class.getDeclaredField("offset");
        packerOffset.setAccessible(true);
        packerBuffer = Packer.class.getDeclaredField("buffer");
        packerBuffer.setAccessible(true);
      } catch (NoSuchFieldException e) {
        e.printStackTrace();
      }
    }

    public void reset(int key, byte[] buf, int offset, int len) {
      try {
        packerOffset.set(packer, 0);
        packerBuffer.set(packer, ThreadLocalData.getBuffer());

        // from MapOperation
        if (policy.flags != 0) {
          Pack.init(packer, null);
          packer.packArrayBegin(5);
          packer.packInt(MapOperation.PUT);
          //key.pack(packer);
          packer.packInt(key);
          //val.pack(packer);
          packer.packParticleBytes(buf, offset, len);
          packer.packInt(policy.attributes);
          packer.packInt(policy.flags);
        } else {
          if (policy.itemCommand == MapOperation.REPLACE) {
            // Replace doesn't allow map attributes because it does not create on non-existing key.
            Pack.init(packer, null);
            packer.packArrayBegin(3);
            packer.packInt(policy.itemCommand);
            //key.pack(packer);
            packer.packInt(key);
            //val.pack(packer);
            packer.packParticleBytes(buf, offset, len);
          } else {
            Pack.init(packer, null);
            packer.packArrayBegin(4);
            packer.packInt(policy.itemCommand);
            //key.pack(packer);
            packer.packInt(key);
            //val.pack(packer);
            packer.packParticleBytes(buf, offset, len);
            packer.packInt(policy.attributes);
          }
        }

        int po = (int) packerOffset.get(packer);
        // TODO - so we're assuming for right now we don't write more than 128kb
        byte[] pb = ThreadLocalData.getBuffer();
        if (val.bytes.length <= po) {
          val.bytes = new byte[po];
        }
        System.arraycopy(pb, 0, val.bytes, 0, po);
        val.len = po;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static void oneOp(Blackhole blackhole, Context context) {
    Key key = new Key(context.namespace, context.setString,
            Arrays.copyOfRange(context.hashes, 0, 12));
    List<Value> values = new ArrayList<Value>();
    values.add(new IntegerValue(1));
    values.add(new IntegerValue(2));
    Operation op = MapOperation.getByKeyList(context.bin, values, MapReturnType.VALUE);
    Record r = context.asClient.operate(null, key, op);
    System.out.println(r);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public static void batchMapRead(Blackhole blackhole, Context context) {

    Key[] keys = new Key[4];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = new Key(context.namespace, context.setString,
              Arrays.copyOfRange(context.hashes, i * 12, (i * 12) + 12));
    }

    List<Value> values = new ArrayList<Value>();
    values.add(new IntegerValue(1));
    values.add(new IntegerValue(2));
    Operation op = MapOperation.getByKeyList(context.bin, values, MapReturnType.VALUE);

    Record[] records = context.asClient.get(null, keys, op);
    System.out.println(Arrays.toString(records));
  }

  public static class BV extends Value {
    private byte[] bytes = new byte[4096];
    private int offset;
    private int len;
    private int type = ParticleType.BLOB;

    public BV() {
    }

    public void set(byte[] buffer, int offset, int len) {
      this.bytes = buffer;
      this.offset = offset;
      this.len = len;
    }

    @Override
    public int estimateSize() {
      return len;
    }

    @Override
    public int write(byte[] buffer, int offset) {
      System.arraycopy(bytes, this.offset, buffer, offset, len);
      return len;
    }

    @Override
    public void pack(Packer packer) {
      packer.packParticleBytes(bytes, offset, len);
    }

    @Override
    public int getType() {
      return type;
    }

    @Override
    public Object getObject() {
      return bytes;
    }

    @Override
    public LuaValue getLuaValue(LuaInstance instance) {
      return new LuaBytes(instance, bytes, type);
    }

    @Override
    public String toString() {
      return Buffer.bytesToHexString(bytes);
    }

    @Override
    public boolean equals(Object other) {
      return false;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }
}
