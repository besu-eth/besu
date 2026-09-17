/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.util.io;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BiFunction;

import org.xerial.snappy.Snappy;

/** The Rolling file writer. */
public class RollingFileWriter implements Closeable {
  private static final long MAX_FILE_SIZE = 1 << 30; // 1 GiB max file size

  private final BiFunction<Integer, Boolean, Path> filenameGenerator;
  private final boolean compressed;
  private int currentSize;
  private int fileNumber;
  private FileOutputStream out;
  private final DataOutputStream index;

  /**
   * Instantiates a new Rolling file writer.
   *
   * @param filenameGenerator the filename generator
   * @param compressed the compressed
   * @throws FileNotFoundException the file not found exception
   */
  public RollingFileWriter(
      final BiFunction<Integer, Boolean, Path> filenameGenerator, final boolean compressed)
      throws FileNotFoundException {
    this.filenameGenerator = filenameGenerator;
    this.compressed = compressed;
    currentSize = 0;
    fileNumber = 0;
    final Path firstOutputFile = filenameGenerator.apply(fileNumber, compressed);
    final Path parentPath = firstOutputFile.getParent();
    if (parentPath != null) {
      try {
        Files.createDirectories(parentPath);
      } catch (final IOException e) {
        final FileNotFoundException fnfe =
            new FileNotFoundException("Unable to create directory for rolling file: " + parentPath);
        fnfe.initCause(e);
        throw fnfe;
      }
    }
    out = new FileOutputStream(firstOutputFile.toFile());

    index = new DataOutputStream(new FileOutputStream(dataFileToIndex(firstOutputFile).toFile()));
  }

  /**
   * Data file to index path.
   *
   * @param dataName the data name
   * @return the path
   */
  public static Path dataFileToIndex(final Path dataName) {
    return Path.of(dataName.toString().replaceAll("(.*)[-.]\\d\\d\\d\\d\\.(.)dat", "$1.$2idx"));
  }

  /**
   * Write bytes.
   *
   * @param bytes the bytes
   * @throws IOException the io exception
   */
  public void writeBytes(final byte[] bytes) throws IOException {
    final byte[] finalBytes;
    if (compressed) {
      finalBytes = Snappy.compress(bytes);
    } else {
      finalBytes = bytes;
    }
    int pos = currentSize;
    currentSize += finalBytes.length;
    if (currentSize > MAX_FILE_SIZE) {
      out.close();
      out = new FileOutputStream(filenameGenerator.apply(++fileNumber, compressed).toFile());
      currentSize = finalBytes.length;
      pos = 0;
    }
    index.writeShort(fileNumber);
    index.writeInt(pos);
    out.write(finalBytes);
  }

  @Override
  public void close() throws IOException {
    out.close();
    index.close();
  }
}
