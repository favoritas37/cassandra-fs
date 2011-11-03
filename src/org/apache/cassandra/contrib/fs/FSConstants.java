package org.apache.cassandra.contrib.fs;

import org.apache.cassandra.contrib.fs.util.Bytes;

public class FSConstants {

	// schema config
	public final static String KeySpace = "FS";
	public final static String FolderCF = "Folder";
	public final static String FileCF = "Files";
	public final static String FolderFlag = "$_Folder_$";

	// attribute
	public final static String TypeAttr = "Type"; // file or folder
	public final static String ContentAttr = "Content";
	public final static String LengthAttr = "Length";
	public final static String LastModifyTime = "LastModifyTime";
	public final static String OwnerAttr = "Owner";
	public final static String GroupAttr = "Group";

	// default owner and group

	public final static byte[] DefaultOwner = Bytes.toBytes("root");
	public final static byte[] DefaultGroup = Bytes.toBytes("supergroup");

	// size limitation
	public static int MaxFileSize = 524288000; // (?) can go up to 2GB
	public static int BlockSize = 5242880;//5 * 1024 * 1024;

	// client property
	public final static String Hosts = "cassandra.client.hosts";
	public final static String ExhaustedPolicy = "";
	public final static String MaxActive = "cassandra.client.maxActive";
	public final static String MaxIdle = "cassandra.client.maxIdle";
	public final static String MaxWaitTimeWhenExhausted = "cassandra.client.maxWaitTimeWhenExhausted";
	public final static String CassandraThriftSocketTimeout = "cassandra.client.cassandraThriftSocketTimeout";
        public final static String CassandraReadConsistency = "cassandra.client.ReadConsistency";
        public final static String CassandraWriteConsistency = "cassandra.client.WriteConsistency";
        public final static String CassandraSynchServerIP = "cassandra.benchmark.synchServ.ip";
        public final static String BenchmarkNumOfSmallFiles = "cassandra.benchmark.numOfSmallFiles";
        public final static String BenchmarkNumOfLargeFiles = "cassandra.benchmark.numOfLargeFiles";
        public final static String BenchmarkWriteOnly = "cassandra.benchmark.writeOnly";
        public final static String BlockSizeConfig = "cassandra.client.blockSize";
        public final static String MaxFileSizeConfig = "cassandra.client.maxFileSize";
}
