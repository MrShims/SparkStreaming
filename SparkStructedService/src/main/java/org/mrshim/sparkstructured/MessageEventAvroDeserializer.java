package org.mrshim.sparkstructured;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class MessageEventAvroDeserializer {
    private final Schema avroSchema;

    public MessageEventAvroDeserializer(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public MessageEventAvro deserialize(byte[] avroData) {
        try {
            org.apache.avro.io.DatumReader<GenericRecord> reader = new org.apache.avro.generic.GenericDatumReader<>(avroSchema);
            org.apache.avro.io.Decoder decoder = org.apache.avro.io.DecoderFactory.get().binaryDecoder(avroData, null);

            GenericRecord record = reader.read(null, decoder);

            // Создайте экземпляр вашего класса MessageEventAvro на основе GenericRecord
            long id = (long) record.get("id");
            String message = record.get("message").toString();

            return new MessageEventAvro(id, message);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
