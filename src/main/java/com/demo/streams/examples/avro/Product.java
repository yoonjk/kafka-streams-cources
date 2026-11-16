package com.demo.streams.examples.avro;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

public class Product extends SpecificRecordBase {
    public static final Schema SCHEMA$ = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"Product\",\"namespace\":\"io.confluent.examples.streams.avro\",\"fields\":[" +
        "{\"name\":\"id\",\"type\":\"long\"}," +
        "{\"name\":\"name\",\"type\":\"string\"}]}"
    );
    private long id;
    private String name = "";
    public Product() { }
    public Product(long id, String name) { this.id = id; this.name = name; }
    @Override public Schema getSchema() { return SCHEMA$; }
    @Override public Object get(int field) {
        switch (field) { case 0: return id; case 1: return name; default: throw new IndexOutOfBoundsException("field=" + field); }
    }
    @Override public void put(int field, Object value) {
        switch (field) { case 0: id = ((Number) value).longValue(); break;
            case 1: name = value.toString(); break;
            default: throw new IndexOutOfBoundsException("field=" + field); }
    }
    public long getId() { return id; }
    public void setId(long id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}
