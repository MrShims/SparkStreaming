package org.mrshim.sparkstructured.spark;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.spark.sql.api.java.UDF1;

import java.io.IOException;


public class DeserializeAvro implements UDF1<byte[],String> {


    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8085/";
    private static final int SCHEMA_ID = 2; // Замените на фактический ID схемы

    @Override
    public String call(byte[] bytes) throws Exception {
        DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(getAvroSchema()); // Замените "schema" на вашу схему Avro
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord record = reader.read(null, decoder);
        return record.toString();

    }

    private Schema getAvroSchema() throws IOException, RestClientException {
        SchemaRegistryClient client = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, 10);
        Schema byID = client.getByID(SCHEMA_ID);
        return byID;

    }

}
