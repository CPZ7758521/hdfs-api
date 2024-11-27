package com.pandora.www.hdfsApi.csvToParquet;

import com.pandora.www.hdfsApi.config.Config;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

public class CsvToParquetToHdfs {

    private static Configuration conf;

    private static Logger LOG = LoggerFactory.getLogger(CsvToParquetToHdfs.class);
    static {
        try {
            URL krb5Url = CsvToParquetToHdfs.class.getClassLoader().getResource(Config.env + "/krb5.conf");
            URL keytabUrl = CsvToParquetToHdfs.class.getClassLoader().getResource(Config.env + "/kerberos.keytab");

            String krb5ConfPath = krb5Url.getPath();
            String keytabUrlPath = keytabUrl.getPath();

            System.setProperty("java.security.krb5.conf",krb5ConfPath);

            conf = new Configuration();

            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);

            conf.addResource(Config.env + "/core-site.xml");
            conf.addResource(Config.env + "/hdfs-site.xml");
            conf.addResource(Config.env + "/hive-site.xml");

            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(Config.kerberosUserName, keytabUrlPath);
        } catch (IOException e) {
            LOG.error("kerberos login failure: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static final String SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"TableBean\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}";
    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

    public static void main(String[] args) {
        String resultPathStr = Config.hdfsDestinationPath + "/result/result.parquet";

        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(new Path(resultPathStr))
        .withSchema(SCHEMA)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withConf(conf)
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
