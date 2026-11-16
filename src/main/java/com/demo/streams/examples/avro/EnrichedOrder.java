package com.demo.streams.examples.avro;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

public class EnrichedOrder extends SpecificRecordBase {
    public static final Schema SCHEMA$ = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"EnrichedOrder\",\"namespace\":\"io.confluent.examples.streams.avro\",\"fields\":[" +
        "{\"name\":\"product\",\"type\":" + Product.SCHEMA$.toString() + "}," +
        "{\"name\":\"customer\",\"type\":" + Customer.SCHEMA$.toString() + "}," +
        "{\"name\":\"order\",\"type\":" + Order.SCHEMA$.toString() + "}]}"
    );
    private Product product;
    private Customer customer;
    private Order order;
    public EnrichedOrder() { }
    public EnrichedOrder(Product product, Customer customer, Order order) {
        this.product = product; this.customer = customer; this.order = order;
    }
    @Override public Schema getSchema() { return SCHEMA$; }
    @Override public Object get(int field) {
        switch (field) { case 0: return product; case 1: return customer; case 2: return order;
            default: throw new IndexOutOfBoundsException("field=" + field); }
    }
    @Override public void put(int field, Object value) {
        switch (field) { case 0: product = (Product) value; break;
            case 1: customer = (Customer) value; break;
            case 2: order = (Order) value; break;
            default: throw new IndexOutOfBoundsException("field=" + field); }
    }
    public Product getProduct() { return product; }
    public void setProduct(Product product) { this.product = product; }
    public Customer getCustomer() { return customer; }
    public void setCustomer(Customer customer) { this.customer = customer; }
    public Order getOrder() { return order; }
    public void setOrder(Order order) { this.order = order; }
}

