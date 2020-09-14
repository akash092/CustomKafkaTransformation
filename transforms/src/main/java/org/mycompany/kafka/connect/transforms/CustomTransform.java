package org.mycompany.kafka.connect.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.common.config.ConfigDef;

import org.apache.kafka.connect.transforms.Transformation;

import org.apache.kafka.connect.errors.DataException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class CustomTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Transform Firebolt App payload to Firebolt ES forma";

    private static final String PURPOSE = "transforming payload";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    @Override
    public void configure(Map<String, ?> props) {
    }
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
    @Override
    public void close() {
    }

    @Override
    public R apply(R record) {
        final Map<String, Object> originalRecord = requireMap(operatingValue(record), PURPOSE);

        final HashMap<String, Object> updatedValue = new HashMap<String, Object>();

        for (Map.Entry<String, Object> entry : originalRecord.entrySet()) {
            final String fieldName = entry.getKey();
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            if (fieldName != "payload") {
                continue;
            }
            //processing payload attribute
            Schema.Type inferredType = ConnectSchema.schemaType(value.getClass());
            if (inferredType == null) {
                throw new DataException("Flatten transformation was passed a value of type " + value.getClass()
                        + " which is not supported by Connect's data API");
            }
            final Map<String, Object> payloadValue = requireMap(entry.getValue(), PURPOSE);
            for (Map.Entry<String, Object> payloadEntry : payloadValue.entrySet()) {
                // now at the level of "to" / "from" / "channel"
                final String payloadSubField = payloadEntry.getKey();
                Object payloadSubValue = payloadEntry.getValue();
                switch (payloadSubField) {
                    case "to":
                        updatedValue.put("To", payloadSubValue);
                        break;
                    case "from":
                        updatedValue.put("From", payloadSubValue);
                        break;
                    case "channel":
                        if (payloadSubValue.toString().equals("sms")) {
                            updatedValue.put("Channel", "SMS");
                        } else if (payloadSubValue.toString().equals("email")) {
                            updatedValue.put("Channel", "Email");
                        } else {
                            updatedValue.put("Channel", payloadSubValue);
                        }
                        break;
                    case "countryISO2":
                        updatedValue.put("Country", payloadSubValue);
                        break;
                    case "app":
                        updatedValue.put("App", payloadSubValue);
                        break;
                    case "status":
                        updatedValue.put("Status", payloadSubValue);
                        break;
                    case "statusMessage":
                        updatedValue.put("StatusMessage", payloadSubValue);
                        break;
                    case "subject":
                        updatedValue.put("Subject", payloadSubValue);
                        break;
                    case "content":
                        updatedValue.put("Content", payloadSubValue);
                        break;
                    case "metadata":
                        updatedValue.put("Metadata", payloadSubValue);
                        break;
                    case "publishedAt":
                        updatedValue.put("PublishedAt", payloadSubValue);
                        break;
                    case "smpId":
                        updatedValue.put("SMPId", payloadSubValue);
                        break;
                    case "additionalAttributes":
                        final Map<String, Object> addAttr = requireMap(payloadSubValue, PURPOSE);
                        for (Map.Entry<String, Object> addAttrEntry : addAttr.entrySet()) {
                            // vendor / correlationID /  requestPayload
                            final String addAttrField = addAttrEntry.getKey();
                            Object addAttrValue = addAttrEntry.getValue();
                            switch (addAttrField) {
                                case "vendor":
                                    updatedValue.put("Vendor", addAttrValue);
                                    break;
                                case "groupId":
                                    updatedValue.put("GroupId", addAttrValue);
                                    break;
                                case "correlationId":
                                    updatedValue.put("CorrelationId", addAttrValue);
                                    break;
                                case "requestPayload":
                                    updatedValue.put("RequestPayload", addAttrValue);
                                    break;
                            }
                        }
                        break;
                }
            }
        }

        return newRecord(record, null, updatedValue);
    }

    protected Object operatingValue(R record) {
        return record.value();
    }

    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
}