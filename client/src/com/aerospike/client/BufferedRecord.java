package com.aerospike.client;

import com.aerospike.client.command.Buffer;

import java.util.Map;

public class BufferedRecord extends Record {

  public byte[] buffer;
  public int bufferIdx;

  /**
   * Initialize record.
   *
   * @param bins
   * @param generation
   * @param expiration
   */
  public BufferedRecord(Map<String, Object> bins, int generation, int expiration) {
    super(bins, generation, expiration);
    buffer = new byte[1024];
  }

  public Object getValueFromBuffer(final String name) {
    if (bufferIdx <= 0) {
      return null;
    }

    int readIdx = 0;
    while (readIdx < bufferIdx) {
      int opSize = Buffer.bytesToInt(buffer, readIdx);
      byte nameSize = buffer[readIdx + 7];
      String bin = Buffer.utf8ToString(buffer, readIdx + 8, nameSize);
      // could be null bins?
      if (name == null && bin != null) {
        continue;
      } else if (name != null && bin == null) {
        continue;
      } else if (!name.equals(bin)) {
        continue;
      }

      // matched
      byte particleType = buffer[readIdx + 5];
      int particleByteSize = opSize - (4 + nameSize);
      readIdx += 4 + 4 + nameSize;
      return Buffer.bytesToParticle(particleType, buffer, readIdx, particleByteSize);
    }
    return null;
  }

  public long byteArrayIndex(final byte[] binName) {
    if (bufferIdx <= 0) {
      return 0;
    }

    int readIdx = 0;
    BinSearch:
    while (readIdx < bufferIdx) {
      int opSize = Buffer.bytesToInt(buffer, readIdx);
      byte nameSize = buffer[readIdx + 7];
      //String bin = Buffer.utf8ToString(buffer, readIdx + 8, nameSize);
      // could be null bins?
      if (binName == null && nameSize > 0) {
        continue;
      } else if (binName != null && nameSize == 0) {
        continue;
      } else {
        int idx = readIdx + 8;
        for (int i = 0; i < binName.length; i++) {
          if (binName[i] != buffer[idx++]) {
            break BinSearch;
          }
        }
      }

      // matched
      byte particleType = buffer[readIdx + 5];
      int particleByteSize = opSize - (4 + nameSize);
      readIdx += 4 + 4 + nameSize;
      long result = ((long) readIdx) << 32;
      result |= particleByteSize;
      return result;
      //return Buffer.bytesToParticle(particleType, buffer, readIdx, particleByteSize);
    }
    return 0;
  }
}
