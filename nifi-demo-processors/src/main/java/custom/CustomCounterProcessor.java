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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

@Tags({"Custom"})
@CapabilityDescription("Create basic counter page")
public class CustomCounterProcessor extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success create counter page")
            .build();


    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Error create counter page")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        AtomicBoolean error = new AtomicBoolean();
        AtomicReference<String> result = new AtomicReference<>(null);

        session.read(flowFile, // Read the flowfile content into a String
                in -> {
                    try {

                        final String contents = IOUtils.toString(in, StandardCharsets.UTF_8);

                        result.set(contents);


                    } catch (Exception e) {
                        getLogger().error(e.getMessage() + " Routing to failure.", e);
                    }
                });


        JSONObject jsonObject = new JSONObject(result.get());

        JSONArray topLevelCounters = jsonObject.getJSONArray("counters");

        Map<String, Integer> counterMap = new HashMap<>();

        if (topLevelCounters != null) {
            List<Object> counterList = topLevelCounters.toList();

            for (Object obj : counterList) {
                Map<String, Integer> map = (Map<String, Integer>) obj;

                if (map == null) {
                    continue;
                }

                counterMap.putAll(map);
            }

        }

        Map<String, Integer> sortedMap = counterMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));


        StringBuilder htmlBuilder = new StringBuilder();
        htmlBuilder.append("<table border=1>");

        for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
            htmlBuilder.append(String.format("<tr><td>%s</td><td>%d</td></tr>",
                    entry.getKey(), entry.getValue()));
        }

        htmlBuilder.append("</table>");

        String html = htmlBuilder.toString();

        result.set(html);

        FlowFile updatedFlowFile = session.write(flowFile, (in, out) -> {
            final String resultString = result.get();
            final byte[] resultBytes = resultString.getBytes(StandardCharsets.UTF_8);

            out.write(resultBytes, 0, resultBytes.length);
            out.flush();
        });
        session.transfer(updatedFlowFile, SUCCESS);
    }

}
