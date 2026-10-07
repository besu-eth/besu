/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.testfuzz.javafuzz;

import org.hyperledger.besu.crypto.MessageDigestFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.jspecify.annotations.Nullable;

/**
 * Ported from <a
 * href="https://gitlab.com/gitlab-org/security-products/analyzers/fuzzers/javafuzz">...</a> because
 * fields like max input size were not configurable.
 */
@SuppressWarnings("CatchAndPrintStackTrace")
public class Corpus {
  private final ArrayList<byte[]> inputs;
  private final int maxInputSize;
  private static final int[] INTERESTING8 = {-128, -1, 0, 1, 16, 32, 64, 100, 127};
  private static final int[] INTERESTING16 = {
    -32768, -129, 128, 255, 256, 512, 1000, 1024, 4096, 32767, -128, -1, 0, 1, 16, 32, 64, 100, 127
  };
  private static final int[] INTERESTING32 = {
    -2147483648,
    -100663046,
    -32769,
    32768,
    65535,
    65536,
    100663045,
    2147483647,
    -32768,
    -129,
    128,
    255,
    256,
    512,
    1000,
    1024,
    4096,
    32767,
    -128,
    -1,
    0,
    1,
    16,
    32,
    64,
    100,
    127
  };
  private @Nullable String corpusPath;
  private int seedLength;

  /**
   * Create a corpus
   *
   * @param dirs The directory to store the corpus files, or {@code null} for an in-memory corpus
   */
  public Corpus(final @Nullable String dirs) {
    this.maxInputSize = 0xc001; // 48k+1
    this.corpusPath = null;
    this.inputs = new ArrayList<>();
    if (dirs != null) {
      String[] arr = dirs.split(",", -1);
      for (String s : arr) {
        File f = new File(s);
        if (!f.exists()) {
          f.mkdirs();
        }
        if (f.isDirectory()) {
          if (this.corpusPath == null) {
            this.corpusPath = f.getPath();
          }
          this.loadDir(f);
        } else {
          try {
            this.inputs.add(Files.readAllBytes(f.toPath()));
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
    this.seedLength = this.inputs.size();
  }

  int getLength() {
    return this.inputs.size();
  }

  private boolean randBool() {
    return ThreadLocalRandom.current().nextBoolean();
  }

  private int rand(final int max) {
    return ThreadLocalRandom.current().nextInt(0, max);
  }

  private void loadDir(final File dir) {
    for (final File f : dir.listFiles()) {
      if (f.isFile()) {
        try {
          this.inputs.add(Files.readAllBytes(f.toPath()));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  byte[] generateInput() throws NoSuchAlgorithmException {
    if (this.seedLength != 0) {
      this.seedLength--;
      return this.inputs.get(this.seedLength);
    }
    if (this.inputs.isEmpty()) {
      byte[] buf = new byte[] {};
      this.putBuffer(buf);
      return buf;
    }
    byte[] buf = this.inputs.get(this.rand(this.inputs.size()));
    return this.mutate(buf);
  }

  void putBuffer(final byte[] buf) throws NoSuchAlgorithmException {
    if (this.inputs.contains(buf)) {
      return;
    }

    this.inputs.add(buf);

    writeCorpusFile(buf);
  }

  private void writeCorpusFile(final byte[] buf) throws NoSuchAlgorithmException {
    if (this.corpusPath != null) {
      MessageDigest md = MessageDigestFactory.create("SHA-256");
      md.update(buf);
      byte[] digest = md.digest();
      String hex = String.format("%064x", new BigInteger(1, digest));
      try (FileOutputStream fos = new FileOutputStream(this.corpusPath + "/" + hex)) {
        fos.write(buf);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private String dec2bin(final int dec) {
    String bin = Integer.toBinaryString(dec);
    String padding = new String(new char[32 - bin.length()]).replace("\0", "0");
    return padding + bin;
  }

  private int exp2() {
    String bin = dec2bin(this.rand((int) Math.pow(2, 32)));
    int count = 0;
    for (int i = 0; i < 32; i++) {
      if (bin.charAt(i) == '0') {
        count++;
      } else {
        break;
      }
    }
    return count;
  }

  int chooseLen(final int n) {
    int x = this.rand(100);
    if (x < 90) {
      return this.rand(Math.min(8, n)) + 1;
    } else if (x < 99) {
      return this.rand(Math.min(32, n)) + 1;
    } else {
      return this.rand(n) + 1;
    }
  }

  static void copy(
      final byte[] src, final int srcPos, final byte[] dst, final int dstPos, final int length) {
    System.arraycopy(src, srcPos, dst, dstPos, Math.min(length, src.length - srcPos));
  }

  static void copy(final byte[] src, final int srcPos, final byte[] dst, final int dstPos) {
    System.arraycopy(src, srcPos, dst, dstPos, Math.min(src.length - srcPos, dst.length - dstPos));
  }

  static byte[] concatZeros(final byte[] a, final int n) {
    byte[] c = new byte[a.length + n];
    Arrays.fill(c, (byte) 0);
    System.arraycopy(a, 0, c, 0, a.length);
    return c;
  }

  byte[] mutate(final byte[] buf) {
    byte[] res = buf.clone();
    int nm = 1 + this.exp2();
    for (int i = 0; i < nm; i++) {
      int x = this.rand(16);
      if (x == 0) {
        // Remove a range of bytes.
        if (res.length <= 1) {
          i--;
          continue;
        }
        int pos0 = this.rand(res.length);
        int pos1 = pos0 + this.chooseLen(res.length - pos0);
        copy(res, pos1, res, pos0, res.length - pos0);
        res = Arrays.copyOfRange(res, 0, res.length - (pos1 - pos0));
      } else if (x == 1) {
        // Insert a range of random bytes.
        int pos = this.rand(res.length + 1);
        int n = this.chooseLen(10);
        res = concatZeros(res, n);
        copy(res, pos, res, pos + n);
        for (int k = 0; k < n; k++) {
          res[pos + k] = (byte) this.rand(256);
        }
      } else if (x == 2) {
        // Duplicate a range of bytes.
        if (res.length <= 1) {
          i--;
          continue;
        }
        int src = this.rand(res.length);
        int dst = this.rand(res.length);
        while (src == dst) {
          dst = this.rand(res.length);
        }
        int n = this.chooseLen(res.length - src);
        byte[] tmp = new byte[n];
        Arrays.fill(tmp, (byte) 0);
        copy(res, src, tmp, 0);
        res = concatZeros(res, n);
        copy(res, dst, res, dst + n);
        System.arraycopy(tmp, 0, res, dst, n);
      } else if (x == 3) {
        // Copy a range of bytes.
        if (res.length <= 1) {
          i--;
          continue;
        }
        int src = this.rand(res.length);
        int dst = this.rand(res.length);
        while (src == dst) {
          dst = this.rand(res.length);
        }
        int n = this.chooseLen(res.length - src);
        copy(res, src + n, res, dst);
      } else if (x == 4) {
        // Bit flip. Spooky!
        if (res.length <= 1) {
          i--;
          continue;
        }
        int pos = this.rand(res.length);
        res[pos] ^= (byte) (1 << (byte) this.rand(8));
      } else if (x == 5) {
        // Set a byte to a random value.
        if (res.length <= 1) {
          i--;
          continue;
        }
        int pos = this.rand(res.length);
        res[pos] ^= (byte) (this.rand(255) + 1);
      } else if (x == 6) {
        // Swap 2 bytes.
        if (res.length <= 1) {
          i--;
          continue;
        }
        int src = this.rand(res.length);
        int dst = this.rand(res.length);
        while (src == dst) {
          dst = this.rand(res.length);
        }
        byte tmp1 = res[src];
        res[src] = res[dst];
        res[dst] = tmp1;
      } else if (x == 7) {
        // Add/subtract from a byte.
        if (res.length == 0) {
          i--;
          continue;
        }
        int pos = this.rand(res.length);
        int v = this.rand(35) + 1;
        if (this.randBool()) {
          res[pos] += (byte) v;
        } else {
          res[pos] -= (byte) v;
        }
      } else if (x == 8) {
        // Add/subtract from a uint16.
        i--;
        //                if (res.length < 2) {
        //                    i--;
        //                    continue;
        //                }
        //                int pos = this.rand(res.length - 1);
        //                int v = this.rand(35) + 1;
        //                if (this.randBool()) {
        //                    v = 0 - v;
        //                }
        //
        //                if (this.randBool()) {
        //                    res[pos] =
        //                } else {
        //
        //                }

      } else if (x == 9) {
        i--;
        // Add/subtract from a uint32.
      } else if (x == 10) {
        // Replace a byte with an interesting value.
        if (res.length == 0) {
          i--;
          continue;
        }
        int pos = this.rand(res.length);
        res[pos] = (byte) INTERESTING8[this.rand(INTERESTING8.length)];
      } else if (x == 11) {
        // Replace an uint16 with an interesting value.
        if (res.length < 2) {
          i--;
          continue;
        }
        int pos = this.rand(res.length - 1);
        if (this.randBool()) {
          res[pos] = (byte) (INTERESTING16[this.rand(INTERESTING16.length)] & 0xFF);
          res[pos + 1] = (byte) ((INTERESTING16[this.rand(INTERESTING16.length)] >> 8) & 0xFF);
        } else {
          res[pos + 1] = (byte) (INTERESTING16[this.rand(INTERESTING16.length)] & 0xFF);
          res[pos] = (byte) ((INTERESTING16[this.rand(INTERESTING16.length)] >> 8) & 0xFF);
        }
      } else if (x == 12) {
        // Replace an uint32 with an interesting value.
        if (res.length < 4) {
          i--;
          continue;
        }
        int pos = this.rand(res.length - 3);
        if (this.randBool()) {
          res[pos] = (byte) (INTERESTING32[this.rand(INTERESTING32.length)] & 0xFF);
          res[pos + 1] = (byte) ((INTERESTING32[this.rand(INTERESTING32.length)] >> 8) & 0xFF);
          res[pos + 2] = (byte) ((INTERESTING32[this.rand(INTERESTING32.length)] >> 16) & 0xFF);
          res[pos + 3] = (byte) ((INTERESTING32[this.rand(INTERESTING32.length)] >> 24) & 0xFF);
        } else {
          res[pos + 3] = (byte) (INTERESTING32[this.rand(INTERESTING32.length)] & 0xFF);
          res[pos + 2] = (byte) ((INTERESTING32[this.rand(INTERESTING32.length)] >> 8) & 0xFF);
          res[pos + 1] = (byte) ((INTERESTING32[this.rand(INTERESTING32.length)] >> 16) & 0xFF);
          res[pos] = (byte) ((INTERESTING32[this.rand(INTERESTING32.length)] >> 24) & 0xFF);
        }
      } else if (x == 13) {
        // Replace an ascii digit with another digit.
        List<Integer> digits = new ArrayList<>();
        for (int k = 0; k < res.length; k++) {
          if (res[k] >= 48 && res[k] <= 57) {
            digits.add(k);
          }
        }
        if (digits.isEmpty()) {
          i--;
          continue;
        }
        int pos = this.rand(digits.size());
        int was = res[digits.get(pos)];
        int now = was;
        while (now == was) {
          now = this.rand(10) + 48;
        }
        res[digits.get(pos)] = (byte) now;
      } else if (x == 14) {
        // Splice another input.
        if (res.length < 4 || this.inputs.size() < 2) {
          i--;
          continue;
        }
        byte[] other = this.inputs.get(this.rand(this.inputs.size()));
        if (other.length < 4) {
          i--;
          continue;
        }
        // Find common prefix and suffix.
        int idx0 = 0;
        while (idx0 < res.length && idx0 < other.length && res[idx0] == other[idx0]) {
          idx0++;
        }
        int idx1 = 0;
        while (idx1 < res.length
            && idx1 < other.length
            && res[res.length - idx1 - 1] == other[other.length - idx1 - 1]) {
          idx1++;
        }
        int diff = Math.min(res.length - idx0 - idx1, other.length - idx0 - idx1);
        if (diff < 4) {
          i--;
          continue;
        }
        copy(other, idx0, res, idx0, Math.min(other.length, idx0 + this.rand(diff - 2) + 1) - idx0);
      } else if (x == 15) {
        // Insert a part of another input.
        if (res.length < 4 || this.inputs.size() < 2) {
          i--;
          continue;
        }
        byte[] other = this.inputs.get(this.rand(this.inputs.size()));
        if (other.length < 4) {
          i--;
          continue;
        }
        int pos0 = this.rand(res.length + 1);
        int pos1 = this.rand(other.length - 2);
        int n = this.chooseLen(other.length - pos1 - 2) + 2;
        res = concatZeros(res, n);
        copy(res, pos0, res, pos0 + n);
        if (n >= 0) System.arraycopy(other, pos1, res, pos0, n);
      }
    }

    if (res.length > this.maxInputSize) {
      res = Arrays.copyOfRange(res, 0, this.maxInputSize);
    }
    return res;
  }
}
