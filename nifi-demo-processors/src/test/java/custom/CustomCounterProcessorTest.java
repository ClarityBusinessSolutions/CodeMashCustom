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
package custom;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


public class CustomCounterProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(CustomCounterProcessor.class);
    }

    @Test
    public void testProcessor() {

        String testJson = "{\n" +
                "  \"counters\" : [ {\n" +
                "    \"foo1\" : 1\n" +
                "  }, {\n" +
                "    \"bar1\" : 1\n" +
                "  }, {\n" +
                "    \"foobar1\" : 3\n" +
                "  }]}";

        testRunner.enqueue(testJson);

        testRunner.run();

        List<MockFlowFile> ffList;
        ffList = testRunner.getFlowFilesForRelationship(CustomCounterProcessor.SUCCESS);

        for (MockFlowFile flowFile: ffList) {
            System.out.println(new String(testRunner.getContentAsByteArray(flowFile)));
        }
    }

}
