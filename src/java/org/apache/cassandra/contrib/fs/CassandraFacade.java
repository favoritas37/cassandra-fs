package org.apache.cassandra.contrib.fs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import me.prettyprint.hector.api.beans.SuperRow;
import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createColumnQuery;
import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createMultigetSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Logger;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.beans.SuperRows;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import org.apache.cassandra.thrift.ColumnPath;
import me.prettyprint.hector.api.exceptions.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.CountQuery;
import me.prettyprint.hector.api.query.MultigetSuperSliceQuery;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import me.prettyprint.hector.api.query.SuperCountQuery;

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
    private final StringSerializer serializer = StringSerializer.get();
    private final BytesArraySerializer byteSerializer = BytesArraySerializer.get();
    private Keyspace keyspace;
    //static String strEncoding = "ASCII";

    public static CassandraFacade getInstance() throws IOException
    {
        if (instance == null) {
            synchronized (CassandraFacade.class) {
                if (instance == null) {
                    instance = new CassandraFacade();
                }
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
        FSConstants.MaxFileSize = conf.getMaxFileSize();
        FSConstants.BlockSize = conf.getBlockSize();

        cluster = getOrCreateCluster("CassandraFS", conf.getHosts());
        keyspace = createKeyspace(FSConstants.KeySpace, cluster);
        
        cLevel = new FSConsistencyLevelPolicy(
                conf.getReadConsistency(),
                conf.getWriteConsistency());
        keyspace.setConsistencyLevelPolicy(cLevel);
    }

    private ColumnPath extractColumnPath(String column) throws IOException
    {
        String[] subColumns = column.split(":");
        ColumnPath columnPath = new ColumnPath();
        columnPath.setColumn_family(subColumns[0]);
        if (subColumns.length == 2) {
            columnPath.setSuper_column((java.nio.ByteBuffer) null);
            columnPath.setColumn(subColumns[1].getBytes());
        }
        else if (subColumns.length == 3) {
            columnPath.setSuper_column(subColumns[1].getBytes());
            columnPath.setColumn(subColumns[2].getBytes());
        }
        else {
            throw new IOException("The column is not the right format:"
                    + column);
        }
        return columnPath;
    }

    public void put(String key, String column, byte[] value) throws IOException
    {
        ColumnPath columnPath = extractColumnPath(column);

        if(columnPath.column_family.compareTo(FSConstants.FileCF) == 0)
            this.insert(key, value, serializer, byteSerializer, columnPath.column_family, new String(columnPath.column.array()));
        else
            this.insert(key, new String(value), columnPath.column_family,
                                                new String(columnPath.super_column.array()),
                                                new String(columnPath.column.array()));

        LOGGER.debug("Bytes written to Cassandra: " + value.length);
    }

    // Support only one superColumnOnly at one time
    public void batchPut(String key, String cfName, String colName, Map<String, byte[]> map,
            boolean isSuperColumn) throws IOException
    {
        if (!isSuperColumn) {
            insertMulti(key, map, serializer, cfName);
        }
        else {
            insertMultiSuper(key, colName, cfName, map, serializer);
        }
    }

    public void batchPutMultipleSimpleKeyColumns(Map<String, Map<String, byte[]>> map, String cfName) throws IOException
    {
        insertMulti(map, cfName, serializer);
    }

    public byte[] get(String key, String column) throws IOException
    {
        ColumnPath columnPath = extractColumnPath(column);

        byte[] result = get(key, serializer, columnPath.column_family, new String(columnPath.getColumn()));
        if(result != null)
            LOGGER.debug("Bytes read from Cassandra: " + result.length);

        return result;
    }

    /**
     * Delete all columns refering to that row
     * @param key
     * @throws IOException
     */
    public void delete(String key) throws IOException
    {
        LOGGER.debug("Deleting key: " + key);
        delete(FSConstants.FileCF, null, serializer, key);
        delete(FSConstants.FolderCF, null, serializer, key);
    }

    /**
     * Delete specific key & column
     * @param key
     * @param columnFamily
     * @param superColumn
     * @throws IOException
     */
    public void delete(String key, String columnFamily, String superColumn)
            throws IOException
    {
        delete(columnFamily, superColumn, serializer, key);
    }

    public boolean exist(String key) throws IOException
    {
        int rowCount = countRow(key, FSConstants.FileCF, serializer);

        LOGGER.debug(key + ": exists = " + rowCount);
        return (rowCount > 0) ? true : false;
    }

    public boolean existDir(String key) throws IOException
    {
        int rowCount = countRowSuper(key, FSConstants.FolderCF, serializer);

        LOGGER.debug(key + ": dir exists = " + rowCount);
        return (rowCount > 0) ? true : false;
    }


    public List<Path> list(String key, String columnFamily,
            boolean includeFolderFlag) throws IOException
    {
        List<Path> children = new ArrayList<Path>();

        if (columnFamily.equals(FSConstants.FolderCF))
        {
            List<HSuperColumn<String, String, String>> attrColumn = getMultiSuper(key, columnFamily, serializer,
                    FSConstants.TypeAttr,
                    FSConstants.LengthAttr,
                    FSConstants.LastModifyTime,
                    FSConstants.OwnerAttr,
                    FSConstants.GroupAttr);

            for (HSuperColumn sc : attrColumn)
            {
                    String name = new String(sc.getNameBytes());
                    List<HColumn<String, String>> attributes = sc.getColumns();
                    Path path = new Path(attributes, name);
                    if (includeFolderFlag) {
                        children.add(path);
                    } else if (!name.equals(FSConstants.FolderFlag)) {
                        children.add(path);
                    }
            }
        }
        else if (columnFamily.equals(FSConstants.FileCF))
        {
            List<HColumn<String, String>> attrColumn = getMultiColumn(key, columnFamily, serializer,
                    FSConstants.TypeAttr,
                    FSConstants.LengthAttr,
                    FSConstants.LastModifyTime,
                    FSConstants.OwnerAttr,
                    FSConstants.GroupAttr);
            
            if (attrColumn.size() != 0)
            {
                Path path = new Path(attrColumn, key);
                children.add(path);
            }

        }
        else {
            throw new RuntimeException("Do not support CF:'" + columnFamily
                    + "' now");
        }

        return children;
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
    public <K,V> void insert(final K key, final V value, Serializer<K> keySerializer, Serializer<V> valueSerializer,
            String cfName, String clName)
    {
        createMutator(keyspace, keySerializer).insert(
                key, cfName, createColumn(clName, value, serializer, valueSerializer));
    }

    private void insert(String key, String value, String cfName, String scName,
                            String cName)
    {
        Mutator<String> mutator =
                HFactory.createMutator(keyspace, serializer);

        mutator.insert(key, cfName,
                HFactory.createSuperColumn(scName,
                    Arrays.asList(HFactory.createStringColumn(cName, value)),
                    serializer, serializer, serializer));
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
            for (Map.Entry<String, byte[]> columnValue : columnValues.get(key).entrySet()) {
                m.addInsertion(key, cfName,
                        createColumn(columnValue.getKey(), columnValue.getValue(),
                        keyspace.createClock(), serializer, byteSerializer));
            }
        }
        m.execute();
    }

    /**
     * Insert multiple columns to one key at a supercolumn
     */
    public <SN, K> void insertMultiSuper(K key, String superColumnName, String cfName,
            Map<String, byte[]> columnValues, Serializer<K> keySerializer)
    {
        Mutator<K> m = createMutator(keyspace, keySerializer);
        List<HColumn<String, String>> columns = new ArrayList<HColumn<String, String>>();

        for (Map.Entry<String, byte[]> columnValue : columnValues.entrySet()) {
            columns.add(HFactory.createStringColumn(columnValue.getKey(), new String(columnValue.getValue())));
        }

        m.addInsertion(key, cfName,
                HFactory.createSuperColumn(superColumnName, columns,
                serializer, serializer, serializer));

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
     * Get Super Column data.
     *
     * @return The string value; null if no value exists for the given key.
     */
    public <K> HSuperColumn<String, String, String> getSuper(final String key, Serializer<K> keySerializer,
            String cfName, String clName) throws HectorException
    {
        SuperColumnQuery<String, String, String, String> superColumnQuery =
                HFactory.createSuperColumnQuery(keyspace, serializer, serializer,
                serializer, serializer);
        superColumnQuery.setColumnFamily(cfName).setKey(key).setSuperName(clName);

        QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();
        return result.get();
    }

    /**
     * Get multiple values from one specific column
     * @param keys
     * @return
     */
    public <K> List<HColumn<String, String>> getMultiKeys(String cfName, String clName, Serializer<K> keySerializer, K... keys)
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
    }

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
        List<HColumn<String, String>> ret = new ArrayList<HColumn<String, String>>();
        
        for (String k : clNames) {
            HColumn<String, String> c = rows.getByKey(key).getColumnSlice().getColumnByName(k);
            if (c != null)
                ret.add(c);
        }
        return ret;
    }

    public <K> List<HSuperColumn<String, String, String>> getMultiSuper(K key, String cfName, Serializer<K> keySerializer, String... clNames)
    {
        MultigetSuperSliceQuery<K, String, String, String> q =
                HFactory.createMultigetSuperSliceQuery(keyspace, keySerializer, serializer, serializer, serializer);
        q.setColumnFamily(cfName);
        q.setKeys(key);
        q.setColumnNames(clNames);
        q.setRange("", "", false, Integer.MAX_VALUE);

        QueryResult<SuperRows<K, String, String, String>> r = q.execute();

        SuperRows<K, String, String, String> rows = r.get();
        //Map<String, String> ret = new HashMap<String, String>(clNames.length);
        List<HSuperColumn<String, String, String>> ret = new ArrayList<HSuperColumn<String, String, String>>();

        Iterator<SuperRow<K, String, String, String>> it = rows.iterator();

        //the iterator is always size 1 ....it has to be.....i suppose...maybe...heaaaa
        while(it.hasNext())
        {
            SuperRow<K, String, String, String> sRow = it.next();
            List<HSuperColumn<String, String, String>> sColumns = sRow.getSuperSlice().getSuperColumns();

            ret.addAll(sColumns);
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

    public <K> int countRowSuper(K key, String cfName, Serializer<K> keySerializer)
    {
        SuperCountQuery<K, String> q = HFactory.createSuperCountQuery(keyspace, keySerializer, serializer);
        QueryResult<Integer> r = q.setKey(key).
                setColumnFamily(cfName).
                setRange("", "", Integer.MAX_VALUE).
                execute();
        return r.get().intValue();
    }
}
