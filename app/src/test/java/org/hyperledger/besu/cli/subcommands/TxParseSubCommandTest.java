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
package org.hyperledger.besu.cli.subcommands;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class TxParseSubCommandTest {

  ByteArrayOutputStream baos = new ByteArrayOutputStream();
  PrintWriter writer = new PrintWriter(new OutputStreamWriter(baos, UTF_8), true);
  TxParseSubCommand txParseSubCommand = new TxParseSubCommand(writer);

  @Test
  public void smokeTest() {
    txParseSubCommand.dump(
        Bytes.fromHexString(
            "0x02f875018363bc5a8477359400850c570bd200825208943893d427c770de9b92065da03a8c825f13498da28702fbf8ccf8530480c080a0dd49141ecf93eeeaed5f3499a6103d6a9778e24a37b1f5c6a336c60c8a078c15a0511b51a3050771f66c1b779210a46a51be521a2446a3f87fc656dcfd1782aa5e"));
    String output = baos.toString(UTF_8);
    assertThat(output).isEqualTo("0xeb2629a2734e272bcc07bda959863f316f4bd4cf\n");
  }

  @Test
  public void assertInvalidRlp() {
    txParseSubCommand.dump(
        Bytes.fromHexString(
            "0x03f90124f9011e010516058261a89403040500000000000000000000000000000000006383000000f8b6f859940000000000000000000000000000000074657374f842a00100000000000000000000000000000000000000000000000000000000000000a00200000000000000000000000000000000000000000000000000000000000000f859940000000000000000000000000000000074657374f842a00100000000000000000000000000000000000000000000000000000000000000a002000000000000000000000000000000000000000000000000000000000000000fc080a082f96cae43a3f2554cad899a6e9168a3cd613454f82e93488f9b4c1a998cc814a06b7519cd0e3159d6ce9bf811ff3ca4e9c5d7ac63fc52142370a0725e4c5273b2c0c0c0"));
    String output = baos.toString(UTF_8);
    assertThat(output).startsWith("err: ");
    assertThat(output).contains("arbitrary precision scalar");
  }

  @Test
  public void assertValidRlp() {
    txParseSubCommand.dump(
        Bytes.fromHexString(
            "0x03f9013f010516058261a89403040500000000000000000000000000000000006383000000f8b6f859940000000000000000000000000000000074657374f842a00100000000000000000000000000000000000000000000000000000000000000a00200000000000000000000000000000000000000000000000000000000000000f859940000000000000000000000000000000074657374f842a00100000000000000000000000000000000000000000000000000000000000000a002000000000000000000000000000000000000000000000000000000000000000fe1a0010657f37554c781402a22917dee2f75def7ab966d7b770905398eba3c44401401a0d98465c5489a09933e6428657f1fc4461d49febd8556c1c7e7053ed0be49cc4fa02c0d67e40aed75f87ea640cc47064d79f4383e24dec283ac6eb81e19da46cc26"));
    String output = baos.toString(UTF_8);
    assertThat(output).isEqualTo("0x730aa72228b55236de378f5267dfc77723b1380c\n");
  }

  @Test
  public void assertInvalidChainId() {
    txParseSubCommand.dump(
        Bytes.fromHexString(
            "0xf901fc303083303030803030b901ae30303030303030303030433030303030303030303030303030303030303030303030303030303030203030413030303030303030303000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000038390030000000002800380000413159000021000000002b0000000000003800000000003800000000000000000000002b633279315a00633200303041374222610063416200325832325a002543323861795930314130383058435931317a633043304239623900310031254363384361000023433158253041380037000027285839005a007937000000623700002761002b5a003937622e000000006300007863410000002a2e320000000000005a0000000000000037242778002039006120412e6362002138300000002a313030303030303030303030373030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030a03030303030303030303030303030303030303030303030303030303030303030a03030303030303030303030303030303030303030303030303030303030303030"));
    String output = baos.toString(UTF_8);
    assertThat(output).isEqualTo("err: wrong chain id\n");
  }

  @Test
  public void corpusFileStreamIsClosedAfterProcessing() throws Exception {
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Path corpus = Files.createTempFile("txparse-corpus", ".txt");
    try {
      Files.write(
          corpus,
          List.of(
              "0x02f875018363bc5a8477359400850c570bd200825208943893d427c770de9b92065da03a8c825f13498da28702fbf8ccf8530480c080a0dd49141ecf93eeeaed5f3499a6103d6a9778e24a37b1f5c6a336c60c8a078c15a0511b51a3050771f66c1b779210a46a51be521a2446a3f87fc656dcfd1782aa5e"),
          UTF_8);

      // Override fileStreamReader to register an onClose handler so we can observe
      // whether run() releases the corpus file stream.
      final TxParseSubCommand cmd =
          new TxParseSubCommand(writer) {
            @Override
            Stream<String> fileStreamReader(final String filePath) {
              return super.fileStreamReader(filePath).onClose(() -> closed.set(true));
            }
          };

      final Field corpusFileField = TxParseSubCommand.class.getDeclaredField("corpusFile");
      corpusFileField.setAccessible(true);
      corpusFileField.set(cmd, corpus.toString());

      cmd.run();

      assertThat(closed.get()).isTrue();
    } finally {
      Files.deleteIfExists(corpus);
    }
  }
}
