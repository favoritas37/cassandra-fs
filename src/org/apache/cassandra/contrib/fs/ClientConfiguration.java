package org.apache.cassandra.contrib.fs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class ClientConfiguration
{

    private final static Logger LOGGER = Logger.getLogger(ClientConfiguration.class);
    private Properties properties;
//	private CassandraClientPoolByHost.ExhaustedPolicy defaultExhaustedPolicy = CassandraClientPoolByHost.ExhaustedPolicy.WHEN_EXHAUSTED_BLOCK;
    private String defaultHosts = "localhost:9160";
    private int defaultMaxActive = 10;
    private int defaultMaxIdle = 10;
    private int defaultMaxWaitTimeWhenExhausted = 60 * 1000;
    private int defaultCassandraThriftSocketTimeout = 60 * 1000;
//    private String consistencyRead = "QUORUM";
//    private String consistencyWrite = "QUORUM";

    public ClientConfiguration(String propertyFile) throws IOException
    {
        try {
            properties = new Properties();
            properties.load(new FileInputStream(propertyFile));
        } catch (FileNotFoundException e) {
            throw new IOException(e);
        }
    }

    public String getReadConsistency()
    {
        String cons = properties.getProperty(FSConstants.CassandraReadConsistency);
        if (cons == null) {
            LOGGER.warn("'" + FSConstants.CassandraReadConsistency
                    + "' is not provided, the default value will been used");
            return defaultHosts;
        }
        else {
            return cons;
        }
    }

    public String getWriteConsistency()
    {
        String cons = properties.getProperty(FSConstants.CassandraWriteConsistency);
        if (cons == null) {
            LOGGER.warn("'" + FSConstants.CassandraReadConsistency
                    + "' is not provided, the default value will been used");
            return defaultHosts;
        }
        else {
            return cons;
        }
    }

    public String getHosts()
    {
        String hosts = properties.getProperty(FSConstants.Hosts);
        if (hosts == null) {
            LOGGER.warn("'" + FSConstants.Hosts
                    + "' is not provided, the default value will been used");
            return defaultHosts;
        }
        else {
            return hosts;
        }
    }

    public int getMaxActive()
    {
        String maxActive = properties.getProperty(FSConstants.MaxActive);
        if (maxActive == null) {
            LOGGER.warn("'" + FSConstants.MaxActive
                    + "' is not provided, the default value will been used");
            return defaultMaxActive;
        }
        else {
            return Integer.parseInt(maxActive);
        }
    }

    public int getMaxIdle()
    {
        String maxIdle = properties.getProperty(FSConstants.MaxIdle);
        if (maxIdle == null) {
            LOGGER.warn("'" + FSConstants.MaxIdle
                    + "' is not provided, the default value will been used");
            return defaultMaxIdle;
        }
        else {
            return Integer.parseInt(maxIdle);
        }
    }

    public int getMaxWaitTimeWhenExhausted()
    {
        String maxWaitTimeWhenExhausted = properties.getProperty(FSConstants.MaxWaitTimeWhenExhausted);
        if (maxWaitTimeWhenExhausted == null) {
            LOGGER.warn("'" + FSConstants.MaxWaitTimeWhenExhausted
                    + "' is not provided, the default value will been used");
            return defaultMaxWaitTimeWhenExhausted;
        }
        else {
            return Integer.parseInt(maxWaitTimeWhenExhausted);
        }
    }

    public int getCassandraThriftSocketTimeout()
    {
        String cassandraThriftSocketTimeout = properties.getProperty(FSConstants.CassandraThriftSocketTimeout);
        if (cassandraThriftSocketTimeout == null) {
            LOGGER.warn("'" + FSConstants.CassandraThriftSocketTimeout
                    + "' is not provided, the default value will been used");
            return defaultCassandraThriftSocketTimeout;
        }
        else {
            return Integer.parseInt(cassandraThriftSocketTimeout);
        }
    }

    public String getBenchmarkSynchServerIp()
    {
        String cons = properties.getProperty(FSConstants.CassandraSynchServerIP);
        if (cons == null) {
            LOGGER.warn("'" + FSConstants.CassandraSynchServerIP
                    + "' is not provided, the default value will been used");
            return "127.0.0.1:9050";
        }
        else {
            return cons;
        }
    }

     public int getBenchmarkNumOfSmallFiles()
    {
        String cons = properties.getProperty(FSConstants.BenchmarkNumOfSmallFiles);
        if (cons == null) {
            LOGGER.warn("'" + FSConstants.BenchmarkNumOfSmallFiles
                    + "' is not provided, the default value will been used");
            return 100;
        }
        else {
            return Integer.parseInt(cons);
        }
    }

    public int getBenchmarkNumOfLargeFiles()
    {
        String cons = properties.getProperty(FSConstants.BenchmarkNumOfLargeFiles);
        if (cons == null) {
            LOGGER.warn("'" + FSConstants.BenchmarkNumOfLargeFiles
                    + "' is not provided, the default value will been used");
            return 10;
        }
        else {
            return Integer.parseInt(cons);
        }
    }
    
    public boolean isWriteOnly()
    {
        String cons = properties.getProperty(FSConstants.BenchmarkWriteOnly);
        if (cons == null) {
            LOGGER.warn("'" + FSConstants.BenchmarkWriteOnly
                    + "' is not provided, the default value will been used");
            return false;
        }
        else {
            if(cons.compareTo("true") == 0)
                return true;
            else
                return false;
        }
    }

    public int getBlockSize()
    {
        String cons = properties.getProperty(FSConstants.BlockSizeConfig);
        if (cons == null) {
            LOGGER.warn("'" + FSConstants.BlockSizeConfig
                    + "' is not provided, the default value will been used");
            return 5242880;
        }
        else 
        {
            int value = 5242880;

            try{ value = Integer.parseInt(cons);}
            catch(NumberFormatException e){}

            return value;
        }
    }

    public int getMaxFileSize()
    {
        String cons = properties.getProperty(FSConstants.MaxFileSizeConfig);
        if (cons == null) {
            LOGGER.warn("'" + FSConstants.MaxFileSizeConfig
                    + "' is not provided, the default value will been used");
            return 524288000;
        }
        else
        {
            int value = 524288000;

            try{ value = Integer.parseInt(cons);}
            catch(NumberFormatException e){}

            return value;
        }
    }

//	public ExhaustedPolicy getExhaustedPolicy() {
//		String exhaustedPolicy = properties
//				.getProperty(FSConstants.ExhaustedPolicy);
//		if (exhaustedPolicy == null) {
//			LOGGER.warn("'" + FSConstants.ExhaustedPolicy
//					+ "' is not provided, the default value will been used");
//			return defaultExhaustedPolicy;
//		} else {
//			return ExhaustedPolicy.valueOf(exhaustedPolicy);
//		}
//	}
    public static void main(String[] args)
    {
        Properties prop = new Properties();
        System.out.println(prop.getProperty("zjf"));
    }
}
