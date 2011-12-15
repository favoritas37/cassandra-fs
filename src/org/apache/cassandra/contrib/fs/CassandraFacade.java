package org.apache.cassandra.contrib.fs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createColumnQuery;
import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import org.apache.log4j.Logger;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;


import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.exceptions.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.CountQuery;
import org.apache.cassandra.contrib.fs.util.Bytes;

/**
 * TODO use the hector java api in future, here use the thrift api just for
 * learning
 * 
 * @author zhanje
 * 
 */
public class CassandraFacade
{
    private FSConsistencyLevelPolicy cLevel;
    private static Logger LOGGER = Logger.getLogger(CassandraFacade.class);
    private static CassandraFacade instance;
    private CassandraHostConfigurator cassandraHostConfigurator;
    private Cluster cluster;
    private final Serializer<byte[]> hectorByteSerializer = BytesArraySerializer.get();
    private final Serializer<String> hectorStringSerializer = StringSerializer.get();
    private final Serializer<Long> hectorLongSerializer = LongSerializer.get();
    private final StringSerializer serializer = StringSerializer.get();
    private final BytesArraySerializer byteSerializer = BytesArraySerializer.get();
    private final DateSerializer dateSerializer = DateSerializer.get();
    private final LongSerializer longSerializer = LongSerializer.get();
    private Keyspace keyspace;

    public static CassandraFacade getInstance() throws IOException
    {
        synchronized (CassandraFacade.class) {
            if (instance == null) {
                instance = new CassandraFacade();
            }
        }
        return instance;
    }

    private CassandraFacade() throws IOException
    {
        File clientConfFile = new File(/*System.getProperty("storage-config")
                + File.separator +*/"client-conf.properties");
        if (!clientConfFile.exists()) {
            throw new RuntimeException("'" + clientConfFile.getAbsolutePath()
                    + "' does not exist!");
        }

        ClientConfiguration conf = new ClientConfiguration(clientConfFile.getAbsolutePath());
        cassandraHostConfigurator = new CassandraHostConfigurator(conf.getHosts() + ":9160");
        cassandraHostConfigurator.setMaxActive(conf.getMaxActive());
        cassandraHostConfigurator.setMaxIdle(conf.getMaxIdle());
        cassandraHostConfigurator.setCassandraThriftSocketTimeout(conf.getCassandraThriftSocketTimeout());
        cassandraHostConfigurator.setMaxWaitTimeWhenExhausted(conf.getMaxWaitTimeWhenExhausted());
        FSConstants.BlockSize = conf.getBlockSize();

        cluster = getOrCreateCluster("CassandraFS", conf.getHosts());
        keyspace = createKeyspace(FSConstants.KeySpace, cluster);
        
        cLevel = new FSConsistencyLevelPolicy(
                conf.getReadConsistency(),
                conf.getWriteConsistency());
        keyspace.setConsistencyLevelPolicy(cLevel);
    }

    ////////////////////////////////////
    /////////// Put Functions   ////////
    ////////////////////////////////////
    public void put(String key, String column, byte[] value, String columnFamily) throws IOException
    {
        this.insert(key, value, byteSerializer, columnFamily, column);
        LOGGER.debug("Bytes written to Cassandra: " + value.length);
    }

    // Support only one superColumnOnly at one time
    public void batchPutSingleRow(String key, String cfName, String colName, Map<String, byte[]> map) throws IOException
    {
        insertMulti(key, map, serializer, cfName);
    }

    public void batchPutMultipleRows(Map<String, Map<String, byte[]>> map, String cfName) throws IOException
    {
        insertMulti(map, cfName, serializer);
    }

    ////////////////////////////////////
    /////////// Get Functions   ////////
    ////////////////////////////////////
    public byte[] get(String columnFamily, String key, String column) throws IOException
    {
        byte[] result = get(key, serializer, columnFamily, column);
        if(result != null)
            LOGGER.debug("Bytes read from Cassandra: " + result.length);

        return result;
    }

    ////////////////////////////////////
    /////////// Delete Functions   /////
    ////////////////////////////////////
    public void delete(String key, String columnFamily) throws IOException
    {
        LOGGER.debug("Deleting key: " + key + ", from CL: " + columnFamily);
        delete(columnFamily, null, serializer, key);
        LOGGER.debug("Data removed: " + key);
    }

    public void delete(String key, String columnFamily, String column)
            throws IOException
    {
        delete(columnFamily, column, serializer, key);
    }

    ////////////////////////////////////
    /////////// Lookup Functions   /////
    ////////////////////////////////////
    public boolean exist(String key, String ColumnFamily) throws IOException
    {
        int rowCount = countRow(key, ColumnFamily, serializer);

        LOGGER.debug(key + ": exists = " + rowCount);
        return (rowCount > 0) ? true : false;
    }
    
    public boolean existsFile(String path) throws IOException
    {
        Path element = new Path(path);
        String query = "SELECT COUNT(*) FROM FileMeta WHERE Name = '"+element.getName()+"' AND Path = '"+element.getParentPath()+"' AND 'Type' = 'File'";
        CqlQuery<String,String,Long> cqlQuery = new CqlQuery<>(keyspace, hectorStringSerializer, hectorStringSerializer, hectorLongSerializer);
        cqlQuery.setQuery(query);
        QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();
        if(result == null) return false;
        return result.get().getAsCount() > 0;
    }

    public boolean existDir(String path) throws IOException
    {
        Path element = new Path(path);
        String query = "SELECT COUNT(*) FROM FileMeta WHERE Name = '"+ element.getName() +"' AND Path = '"+element.getParentPath()+"' AND 'Type' = 'Folder'";
        CqlQuery<String,String,Long> cqlQuery = new CqlQuery<>(keyspace, hectorStringSerializer, hectorStringSerializer, hectorLongSerializer);
        cqlQuery.setQuery(query);
        QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();
        if(result == null) return false;
        return result.get().getAsCount() > 0;
    }

    public List<Path> list(String path, boolean listFolder) throws IOException
    {
        List<Path> children = new ArrayList<>();

        String query = "";
        if (listFolder)
            query = "SELECT * FROM FileMeta WHERE Path = '"+path+"'";
        else //search for a specific file
        {
            Path element = new Path(path);
            query = "SELECT * FROM FileMeta WHERE Name = '"+element.getName()+"' AND Path = '"+element.getParentPath()+"'";
        }
            
        CqlQuery<String,String,byte[]> cqlQuery = new CqlQuery<>(keyspace, hectorStringSerializer, hectorStringSerializer, hectorByteSerializer);
        cqlQuery.setQuery(query);
        QueryResult<CqlRows<String,String,byte[]>> result = cqlQuery.execute();
        if(result == null)
            return children;
        try{
        List<Row<String, String, byte[]>> rows = result.get().getList();
        for (Row<String, String, byte[]> row : rows)
        {
            String uuid = row.getKey();
            List<HColumn<String, byte[]>> attributes = extractInfoFromRow(row);
            Path fileInfo = new Path(attributes, uuid);
            children.add(fileInfo);
        }
        }
        catch(java.lang.NullPointerException e){}
        
        return children;
    }
    
    public int countDirChildren(String path)
    {
        //check here the shlash at the end!!!!!!!!
        String query = "SELECT COUNT(*) FROM FileMeta WHERE Path = '"+path+"'";
        CqlQuery<String,String,Long> cqlQuery = new CqlQuery<>(keyspace, hectorStringSerializer, hectorStringSerializer, hectorLongSerializer);
        cqlQuery.setQuery(query);
        QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();
        if(result == null)
            return 0;
        else
            return result.get().getAsCount();
    }
    
    private List<HColumn<String, byte[]>> extractInfoFromRow(Row<String, String, byte[]> row)
    {
        List<HColumn<String, byte[]>> columns = new ArrayList<>();
        columns.add(row.getColumnSlice().getColumnByName("Name"));
        columns.add(row.getColumnSlice().getColumnByName("Path"));
        columns.add(row.getColumnSlice().getColumnByName("Type"));
        columns.add(row.getColumnSlice().getColumnByName("Length"));
        columns.add(row.getColumnSlice().getColumnByName("CreationTime"));
        columns.add(row.getColumnSlice().getColumnByName("LastModifiedTime"));
        columns.add(row.getColumnSlice().getColumnByName("Owner"));
        columns.add(row.getColumnSlice().getColumnByName("Group"));
        columns.add(row.getColumnSlice().getColumnByName("Permissions"));
        return columns;
    }

    ///////////////////////////
    /// From Hector API     ///
    ///////////////////////////
    /**
     * Insert a new value keyed by key
     *
     * @param key   Key for the value
     * @param value the String value to insert
     */
    public <V> void insert(final String key, final V value, Serializer<V> valueSerializer,
            String cfName, String clName)
    {
        createMutator(keyspace, serializer).insert(
                key, cfName, createColumn(clName, value, serializer, valueSerializer));
    }

    /**
     * Insert multiple columns to one key
     */
    public <K> void insertMulti(K key, Map<String, byte[]> columnValues, Serializer<K> keySerializer,
            String cfName)
    {
        Mutator<K> m = createMutator(keyspace, keySerializer);
        for (Map.Entry<String, byte[]> columnValue : columnValues.entrySet()) {
            m.addInsertion(key, cfName,
                    createColumn(columnValue.getKey(), columnValue.getValue(),
                    keyspace.createClock(), serializer, byteSerializer));
        }
        m.execute();
    }

    /**
     * Insert multiple columns to multiple keys
     */
    public <K> void insertMulti(Map<K, Map<String, byte[]>> columnValues, String cfName, Serializer<K> keySerializer)
    {
        Mutator<K> m = createMutator(keyspace, keySerializer);

        Iterator<K> it = columnValues.keySet().iterator();

        while(it.hasNext())
        {
            K key = it.next();
            for (Map.Entry<String, byte[]> columnValue : columnValues.get(key).entrySet()) 
            {
                if(columnValue.getKey().equals("Length") || 
                        columnValue.getKey().equals("CreationTime") || 
                        columnValue.getKey().equals("LastModifyTime"))
                {
                    m.addInsertion(key, cfName,
                        createColumn(columnValue.getKey(), (long)Bytes.toLong(columnValue.getValue()),
                        keyspace.createClock(), serializer, longSerializer));
                }
                else
                    m.addInsertion(key, cfName,
                            createColumn(columnValue.getKey(), columnValue.getValue(),
                            keyspace.createClock(), serializer, byteSerializer));
            }
        }
        m.execute();
    }

    /**
     * Get a string value.
     *
     * @return The string value; null if no value exists for the given key.
     */
    public <K> byte[] get(final K key, Serializer<K> keySerializer,
            String cfName, String clName) throws HectorException
    {
        ColumnQuery<K, String, byte[]> q = createColumnQuery(keyspace, keySerializer, serializer, byteSerializer);
        QueryResult<HColumn<String, byte[]>> r = q.setKey(key).
                setName(clName).
                setColumnFamily(cfName).
                execute();
        HColumn<String, byte[]> c = r.get();
        return c == null ? null : c.getValue();
    }

    /**
     * Get multiple values from one specific column
     * @param keys
     * @return
     */
    /*public <K> List<HColumn<String, String>> getMultiKeys(String cfName, String clName, Serializer<K> keySerializer, K... keys)
    {
        MultigetSliceQuery<K, String, String> q = createMultigetSliceQuery(keyspace, keySerializer, serializer, serializer);
        q.setColumnFamily(cfName);
        q.setKeys(keys);
        q.setColumnNames(clName);

        QueryResult<Rows<K, String, String>> r = q.execute();
        Rows<K, String, String> rows = r.get();
        //Map<K, String> ret = new HashMap<K, String>(keys.length);
        List<HColumn<String, String>> ret = new ArrayList<HColumn<String, String>>();

        for (K k : keys)
        {
            HColumn<String, String> c = rows.getByKey(k).getColumnSlice().getColumnByName(clName);
            if (c != null)
                ret.add(c);
        }
        return ret;
    }*/

    /**
     * Get multiple values from one specific column
     * @param keys
     * @return
     */
    public <K> List<HColumn<String, String>> getMultiColumn(K key, String cfName, Serializer<K> keySerializer, String... clNames)
    {
        MultigetSliceQuery<K, String, String> q = HFactory.createMultigetSliceQuery(keyspace, keySerializer, serializer, serializer);
        q.setColumnFamily(cfName);
        q.setKeys(key);
        q.setColumnNames(clNames);

        QueryResult<Rows<K, String, String>> r = q.execute();
        Rows<K, String, String> rows = r.get();
        //Map<String, String> ret = new HashMap<String, String>(clNames.length);
        List<HColumn<String, String>> ret = new ArrayList<>();
        
        for (String k : clNames) {
            HColumn<String, String> c = rows.getByKey(key).getColumnSlice().getColumnByName(k);
            if (c != null)
                ret.add(c);
        }
        return ret;
    }

    /**
     * Delete multiple values
     *
     * if clName is null then all columns are deleted
     */
    public <K> void delete(String cfName, String clName, Serializer<K> keySerializer, K... keys)
    {
        Mutator<K> m = createMutator(keyspace, keySerializer);
        for (K key : keys) {
            m.addDeletion(key, cfName, clName, serializer);
        }
        m.execute();
    }

    public <K> int countRow(K key, String cfName, Serializer<K> keySerializer)
    {
        CountQuery<K, String> q = HFactory.createCountQuery(keyspace, keySerializer, serializer);
        QueryResult<Integer> r = q.setKey(key).
                setColumnFamily(cfName).
                setRange("", "", Integer.MAX_VALUE).
                execute();
        return r.get().intValue();
    }
    
///////////////////////////////////////////////////////////////
//////  CQL functions
    
    /**
     * There might be problem here because the function will be called at every lookup to 
     * get the UUID of the file. Could be extended to have a cashe but that will be investigated
     * later on.
     * @param path
     * @return 
     */
   
    public String getRowUUID(String path)
    {
        Path element = new Path(path);
        String query = "SELECT Name FROM FileMeta WHERE Name = '"+element.getName()+"' AND Path = '"+element.getParentPath()+"'";
        
        CqlQuery<String,String,String> cqlQuery = 
                new CqlQuery<>(keyspace, hectorStringSerializer, hectorStringSerializer, hectorStringSerializer);
        cqlQuery.setQuery(query);
        QueryResult<CqlRows<String,String,String>> result = cqlQuery.execute();
        if(result == null)
            return "";
        
        try{
            List<Row<String, String, String>> rows = result.get().getList();

            if(rows.size() > 0)
                return rows.get(0).getKey();
        }
        catch(NullPointerException e){LOGGER.debug("No UUID found for path: " +path );}
            
        return "";
    }
    
    public static void main(String[] args) throws IOException
    {
        CassandraFacade fs = CassandraFacade.getInstance();
        String uuid = UUID.randomUUID().toString();
        System.out.println("UUID: " + uuid);
        fs.put(uuid, "Name", Bytes.toBytes(new String("testFile")), FSConstants.FileMetaCF);
        fs.put(uuid, "Path", Bytes.toBytes(new String("/usr/ftylitak")), FSConstants.FileMetaCF);
        String uuidNew = fs.getRowUUID("/usr/ftylitak/testFile");
        System.out.println("UUID new: " + uuidNew);
        
        return;
    }
}
