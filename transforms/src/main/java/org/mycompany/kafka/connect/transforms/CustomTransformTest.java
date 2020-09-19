package org.mycompany.kafka.connect.transforms;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CustomTransformTest {
    private final CustomTransform<SourceRecord> xform = new CustomTransform<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void testSampleData () {
        Map<String, Object> requestPayload = new HashMap<>();
        requestPayload.put("groupId", 12345);

        Map<String, Object> parent = new HashMap<>();
        parent.put("channel", "sms");
        parent.put("additional", (Object)requestPayload);

        Map<String, Object> oneLevelNestedMap = Collections.singletonMap("payload", (Object) parent);

        SourceRecord transformed = xform.apply(new SourceRecord(null, null,
                "topic", 0,
                null, oneLevelNestedMap));

        assertNull(transformed.valueSchema());
        assertTrue(transformed.value() instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedMap = (Map<String, Object>) transformed.value();
        assertEquals("SMS", transformedMap.get("Channel"));
        assertEquals(12345, transformedMap.get("GroupId"));
    }
}
