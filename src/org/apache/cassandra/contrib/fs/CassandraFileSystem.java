package org.apache.cassandra.contrib.fs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.contrib.fs.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

/**
 * The path here must be absolute, the relative path is handled by FSCliMain
 * 
 * @author zhanje
 * 
 */
public class CassandraFileSystem implements IFileSystem {

    private static Logger LOGGER = Logger.getLogger(CassandraFileSystem.class);
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm");
    private static IFileSystem instance;
    private CassandraFacade facade;
    
    public static IFileSystem getInstance() throws TTransportException,
            IOException {
        synchronized (CassandraFileSystem.class) {
            if (instance == null) {
                instance = new CassandraFileSystem();
            }
        }
        return instance;
    }

    private CassandraFileSystem() throws TTransportException, IOException {
        this.facade = CassandraFacade.getInstance();
        if (!existDir("/")) {
            mkdir("/");
        }
    }

    public void createFile(String path, byte[] content) throws IOException {
        createFile(path, new ByteArrayInputStream(content) );
    }

    @Override
    public void createFile(String path, InputStream in) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        Path filePath = new Path(path);
        if (!existDir(filePath.getParentPath())) {
            mkdir(filePath.getParentPath());
        }
        
        //This will be the unique identifier of the file which will define the row
        //where the data will be stored.
        String checkPrevUUID = facade.getRowUUID(path);
        String fileUUID = "";
        if(checkPrevUUID == "")
            fileUUID = generateUUID();
        else
            fileUUID = checkPrevUUID;

        Map<String, Map<String, byte[]>> metaData = new HashMap<>();
        long length = 0;
        int index = 0;
        int num = 0;
        while (true) {
            byte[] buffer = new byte[FSConstants.BlockSize];
            num = in.read(buffer);
            LOGGER.debug("Read number of bytes: " + num + " from: " + path);
            if (num == -1) {break;}
            
            byte[] content;
            if(num == FSConstants.BlockSize)
                content = buffer;
            else
            {
                content = new byte[num];
                System.arraycopy(buffer, 0, content, 0, num);
            }
            
            length += num;
                           
            LOGGER.debug("Putting path: " + fileUUID + "_$" + index);
            facade.put(fileUUID + "_$" + index, FSConstants.ChunkAttr, content, FSConstants.FileDataCF);
            index++;
        }
        LOGGER.debug("Data imported successfully: " + filePath.getName());
        LOGGER.debug("Byte length: " + length + ", " + Bytes.toBytes(length));
        
        byte[] date = Bytes.toBytes(new Date().getTime());
        
        Map<String, byte[]> map = new HashMap<>();
        map.put(FSConstants.NameAttr, Bytes.toBytes(filePath.getName()));
        map.put(FSConstants.PathAttr, Bytes.toBytes(filePath.getParentPath()));
        map.put(FSConstants.TypeAttr, Bytes.toBytes("File"));
        map.put(FSConstants.LengthAttr, Bytes.toBytes(length));
        map.put(FSConstants.CreationTimeAttr, date);
        map.put(FSConstants.LastModifiedTime, date);
        map.put(FSConstants.OwnerAttr, FSConstants.DefaultOwner);
        map.put(FSConstants.GroupAttr, FSConstants.DefaultGroup);
        map.put(FSConstants.PermissionsAttr, Bytes.toBytes(0x0777));
        metaData.put(fileUUID, map);

        facade.batchPutMultipleRows(metaData, FSConstants.FileMetaCF);
        LOGGER.debug("Metadata created successfully: " + filePath.getName());
    }

    public boolean deleteFile(String path) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        if (!existFile(path)) {
            LOGGER.warn("File '" + path
                    + "' can not been deleted, because it doesn't exist");
            return false;
        }
        
        String uuid = facade.getRowUUID(path);
        
        facade.delete(uuid, FSConstants.FileMetaCF);
        for (int i = 0; facade.exist(uuid + "_$" + i, FSConstants.FileDataCF); i++) 
            facade.delete(uuid + "_$" + i, FSConstants.FileDataCF);        

        return true;
    }

    public boolean deleteDir(String path, boolean recursive) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        if (!existDir(path)) {
            LOGGER.warn("Folder '" + path
                    + "' can not been deleted, because it doesn't exist");
            return false;
        }
        if (!recursive) {
            List<Path> paths = list(path);
            if (paths.size() > 0) {
                LOGGER.warn("Folder '" + path
                        + "' is not empty, and can not been deleted");
                return false;
            } else {
                String uuid = facade.getRowUUID(path);
                facade.delete(uuid, FSConstants.FileMetaCF);
                return true;
            }
        } else {
            List<Path> paths = list(path);
            for (Path p : paths) {
                if (p.isDir()) {
                    deleteDir(p.getURL(), true);
                } else {
                    deleteFile(p.getURL());
                }
            }
            deleteDir(path, false);
            return true;
        }
    }

    @Override
    public InputStream readFile(String path) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        LOGGER.debug("Reading file '" + path + "'");
        return new CFileInputStream(path, facade);
    }

    public boolean mkdir(String path) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        if (existDir(path)) {
            LOGGER.warn("'" + path + "' is already existed");
            return false;
        }
        
        //recurcive creation of all the parent folders in case they dont exist
        Path folderPath = new Path(path);
        String parent = folderPath.getParentPath();
        if (parent != null && !parent.equals("") && !existDir(parent))
            mkdir(parent);

        if (parent != null) 
        {
            Map<String, Map<String, byte[]>> metaData = new HashMap<>();
            String uuid = generateUUID();
            
            byte[] date = Bytes.toBytes(new Date().getTime());
            
            Map<String, byte[]> map = new HashMap<>();
            map.put(FSConstants.NameAttr, Bytes.toBytes(folderPath.getName()));
            map.put(FSConstants.PathAttr, Bytes.toBytes(parent));
            map.put(FSConstants.TypeAttr, Bytes.toBytes("Folder"));
            map.put(FSConstants.LengthAttr, Bytes.toBytes((long)0));
            map.put(FSConstants.CreationTimeAttr, date);
            map.put(FSConstants.LastModifiedTime, date);
            map.put(FSConstants.OwnerAttr, FSConstants.DefaultOwner);
            map.put(FSConstants.GroupAttr, FSConstants.DefaultGroup);
            map.put(FSConstants.PermissionsAttr, Bytes.toBytes(0x0777));
            metaData.put(uuid, map);

            facade.batchPutMultipleRows(metaData, FSConstants.FileMetaCF);
            LOGGER.debug("Metadata created successfully: " + folderPath.getName());
        }
        else if(parent == null && path.equals("/"))//if creating the root folder '/'
        {
            Map<String, Map<String, byte[]>> metaData = new HashMap<>();
            String uuid = generateUUID();
            byte[] date = Bytes.toBytes(new Date().getTime());
            
            Map<String, byte[]> map = new HashMap<>();
            map.put(FSConstants.NameAttr, Bytes.toBytes("/"));
            map.put(FSConstants.PathAttr, Bytes.toBytes(""));
            map.put(FSConstants.TypeAttr, Bytes.toBytes("Folder"));
            map.put(FSConstants.LengthAttr, Bytes.toBytes((long)0));
            map.put(FSConstants.CreationTimeAttr, date);
            map.put(FSConstants.LastModifiedTime, date);
            map.put(FSConstants.OwnerAttr, FSConstants.DefaultOwner);
            map.put(FSConstants.GroupAttr, FSConstants.DefaultGroup);
            map.put(FSConstants.PermissionsAttr, Bytes.toBytes(0x0777));
            metaData.put(uuid, map);

            facade.batchPutMultipleRows(metaData, FSConstants.FileMetaCF);
            LOGGER.debug("Metadata created successfully: " + "/");
        }
        LOGGER.debug("Create dir '" + path + "' succesfully");
        return true;
    }

    public List<Path> list(String path) throws IOException {
        PathUtil.checkPath(path);
        List<Path> result = new ArrayList<>();
        path = PathUtil.normalizePath(path);
        if (existDir(path)) {
            result = facade.list(path, true);
        } else if (existFile(path)) {
            result = facade.list(path, false);
        } else {
            return result;
        }

        // set the max size length, the max size length is for cli formatting
        int max = 0;
        for (Path child : result) {
            if ((child.getLength() + "").length() > max) {
                max = (child.getLength() + "").length();
            }
        }
        Path.MaxSizeLength = max;

        return result;
    }

    public boolean existDir(String path) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        return facade.existDir(path);
    }

    public boolean existFile(String path) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        return facade.existsFile(path);
    }

    public boolean exist(String path) throws IOException {
        return existDir(path) || existFile(path);
    }

    public int countDirChilden(String path) throws IOException
    {
        return facade.countDirChildren(path);
    }

    public void touchFile(String path) throws IOException
    {
        if(existFile(path))
        {
            String uuid = facade.getRowUUID(path);
            facade.put(uuid, FSConstants.LastModifiedTime, Bytes.toBytes(new Date().getTime()), FSConstants.FileMetaCF);
        }
        else
            createFile(path, "".getBytes());
    }
    
    private String generateUUID()
    {
        return UUID.randomUUID().toString();
    }

    public static void main(String[] args) throws IOException,
            TTransportException {
        System.setProperty("storage-config", "conf");
        IFileSystem fs = CassandraFileSystem.getInstance();
    }
}
