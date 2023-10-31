package org.mrshim.sparkstructured;

import lombok.ToString;
import org.apache.avro.generic.GenericRecord;

@ToString
public class MyDTO {
    private long id;
    private String message;

    public MyDTO() {
    }

    public MyDTO(GenericRecord record) {
        this.id = (long) record.get("id");
        this.message = record.get("message").toString();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
