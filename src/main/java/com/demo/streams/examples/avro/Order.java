package com.demo.streams.examples.avro;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

public class Order extends SpecificRecordBase {
    public static final Schema SCHEMA$ = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.demo.streams.examples.avro\",\"fields\":[" +
        "{\"name\":\"id\",\"type\":\"long\"}," +
        "{\"name\":\"customerId\",\"type\":\"long\"}," +
        "{\"name\":\"productId\",\"type\":\"long\"}]}"
    );
    private long id;
    private long customerId;
    private long productId;
    public Order() { }
    public Order(long id, long customerId, long productId) {
        this.id = id; this.customerId = customerId; this.productId = productId;
    }
    @Override public Schema getSchema() { return SCHEMA$; }
    @Override public Object get(int field) {
        switch (field) { case 0: return id; case 1: return customerId; case 2: return productId;
            default: throw new IndexOutOfBoundsException("field=" + field); }
    }
    @Override public void put(int field, Object value) {
        switch (field) { case 0: id = ((Number) value).longValue(); break;
            case 1: customerId = ((Number) value).longValue(); break;
            case 2: productId = ((Number) value).longValue(); break;
            default: throw new IndexOutOfBoundsException("field=" + field); }
    }
    public long getId() { return id; }
    public void setId(long id) { this.id = id; }
    public long getCustomerId() { return customerId; }
    public void setCustomerId(long customerId) { this.customerId = customerId; }
    public long getProductId() { return productId; }
    public void setProductId(long productId) { this.productId = productId; }
}
