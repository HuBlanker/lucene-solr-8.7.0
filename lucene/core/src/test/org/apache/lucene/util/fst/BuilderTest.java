/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.util.fst;


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;

public class BuilderTest extends LuceneTestCase {

  public void testAdd() throws IOException {
    System.out.println("start");

    Outputs<Object> outputs = NoOutputs.getSingleton();
    Object NO_OUTPUT = outputs.getNoOutput();
    final Builder<Object> b = new Builder<>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs,
        true, 15);

    String[] inputValues = {"mop", "moth", "pop", "star", "stop", "top"};
//    long[] outputValues = {100, 91, 72, 83, 54, 55};


    for (int i = 0; i < 6; i++) {
      char[] chars = inputValues[i].toCharArray();
      int[] tmps = new int[chars.length];
      for (int j = 0; j < chars.length; j++) {
        tmps[j] = chars[j];
      }
      b.add(new IntsRef(tmps, 0, chars.length), NO_OUTPUT);
      System.out.println(Arrays.toString(tmps));

    }

    FST<Object> finish = b.finish();


    IntsRefFSTEnum<Object> fstEnum = new IntsRefFSTEnum<>(finish);

    while (true) {
      IntsRefFSTEnum.InputOutput<Object> next = fstEnum.next();
      if (next == null) break;
      System.out.println(Arrays.toString(next.input.ints));
      System.out.println(next.output);

    }
    System.out.println("done");
  }

  public void printFST(FST<Object> fst) {

  }

}