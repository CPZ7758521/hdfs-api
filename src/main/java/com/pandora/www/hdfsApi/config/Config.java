package com.pandora.www.hdfsApi.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class Config {
    public static String env;
    public static String kerberosUserName;
    public static String apiUser;
    public static String apiPassword;
    public static String hdfsSourcePath;
    public static String hdfsDestinationPath;

    private static Logger LOG = LoggerFactory.getLogger(Config.class);

    static {
        Properties properties = new Properties();

        env = System.getProperty("env");

        try {
            switch (env) {
                case "test":
                    properties.load(Config.class.getClassLoader().getResourceAsStream(env + "/config.properties"));
                case "prod":
                    properties.load(Config.class.getClassLoader().getResourceAsStream(env + "/config.properties"));
            }

            kerberosUserName = properties.getProperty("kerberos.username");

            hdfsSourcePath = properties.getProperty("hdfs.source.path");

            hdfsDestinationPath = properties.getProperty("hdfs.destination.path");



        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
