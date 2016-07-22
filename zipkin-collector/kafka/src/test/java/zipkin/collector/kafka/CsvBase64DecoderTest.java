/**
 * Copyright 2015-2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.collector.kafka;

import org.junit.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvBase64DecoderTest {
  private byte[] value1 = {0xa, 0x2, 0xf};
  private byte[] value2 = {0xd, 0x3, 0x1};

  @Test
  public void testOneValue() {
    byte[] input = Base64.getEncoder().encode(value1);
    List<byte[]> result = CsvBase64Decoder.decode(input);

    assertThat(result).usingElementComparator((a, b) -> Arrays.equals(a, b) ? 0 : 1)
        .isEqualTo(Collections.singletonList(value1));
  }

  @Test
  public void testTwoValues() {
    String input1 = Base64.getEncoder().encodeToString(value1);
    String input2 = Base64.getEncoder().encodeToString(value2);
    byte[] input = (input1 + "," + input2).getBytes();

    List<byte[]> result = CsvBase64Decoder.decode(input);

    assertThat(result).usingElementComparator((a, b) -> Arrays.equals(a, b) ? 0 : 1)
      .isEqualTo(Arrays.asList(value1, value2));
  }

}
