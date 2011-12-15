package org.apache.cassandra.contrib.fs;

import org.apache.cassandra.contrib.fs.util.Bytes;

public class FSConstants {

	// schema config
	public final static String KeySpace = "FS";
	public final static String FileMetaCF = "FileMeta";
        public final static String FileDataCF = "FileData";
	//public final static String FolderFlag = "$_Folder_$";

	// attribute
        public final static String NameAttr = "Name";
        public final static String PathAttr = "Path";
	public final static String TypeAttr = "Type"; // file or folder
	public final static String LengthAttr = "Length";
        public final static String CreationTimeAttr = "CreationTime";
	public final static String LastModifiedTime = "LastModifiedTime";
	public final static String OwnerAttr = "Owner";
	public final static String GroupAttr = "Group";
        public final static String PermissionsAttr = "Permissions";
        
        public final static String ChunkAttr = "Chunk";

	// default owner and group

	public final static byte[] DefaultOwner = Bytes.toBytes("root");
	public final static byte[] DefaultGroup = Bytes.toBytes("supergroup");

	// size limitation
	public static int BlockSize = 15204352;//MAX able 14,5 MB/chunk

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
