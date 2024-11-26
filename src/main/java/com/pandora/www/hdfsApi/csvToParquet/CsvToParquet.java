package com.pandora.www.hdfsApi.csvToParquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class CsvToParquet {
    private static final String SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"TableBean\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}";
    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

    public static void main(String[] args) {
        String resultPathStr = "E:\\data\\result.parquet";
        File resultParquetFile = new File(resultPathStr);
        if (resultParquetFile.exists()) {
            resultParquetFile.delete();
        }
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(new Path(resultPathStr))
        .withSchema(SCHEMA)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
            String sourcePathStr = "E:\\data\\sourceCsv";
            File file = new File(sourcePathStr);
            File[] files = file.listFiles();
            for (File oneFile : files) {
                FileReader fileReader = new FileReader(oneFile);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    GenericData.Record record = new GenericData.Record(SCHEMA);
                    String[] split = line.split(",");
                    record.put("id", split[0]);
                    record.put("name", split[1]);
                    writer.write(record);
                }
                bufferedReader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
