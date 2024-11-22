package com.pandora.www.hdfsApi.fileUtils;

import com.pandora.www.hdfsApi.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kerby.config.Conf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class MyFileUtil {
    private static Configuration conf;

    private static Logger LOG = LoggerFactory.getLogger(MyFileUtil.class);
    static {
        try {
            URL krb5Url = MyFileUtil.class.getClassLoader().getResource(Config.env + "/krb5.conf");
            URL keytabUrl = MyFileUtil.class.getClassLoader().getResource(Config.env + "kerberos.keytab");

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

    public static void deleteFile(String tableName) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(Config.hdfsSourcePath + tableName);

        if (fs.exists(path)) {
            fs.delete(path, false);
        }
        fs.close();
    }

    public static void copyFiles() throws IOException {
        Path sourcePath = new Path(Config.hdfsSourcePath);
        Path destinationPath = new Path(Config.hdfsDestinationPath);

        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(destinationPath)) {
            fs.mkdirs(destinationPath);
        }

        FileUtil.copy(fs, sourcePath, fs, destinationPath, false, conf);

        fs.close();
    }

    public static void uploadFiles(String hdfsPath, String localPath) throws IOException {
        Path destinationPath = new Path(hdfsPath);
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(destinationPath)) {
            fs.mkdirs(destinationPath);
        }

        File file = new File(localPath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File file1 : files) {
                Path sourcePath = new Path(localPath + file1.getName());
                fs.copyFromLocalFile(sourcePath, destinationPath);
            }
        }

        fs.close();
    }
}
