package org.apache.cassandra.contrib.fs;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import me.prettyprint.cassandra.serializers.StringSerializer;

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
    private static SimpleDateFormat format = new SimpleDateFormat(
            "yyyy/MM/dd HH:mm");
    private static IFileSystem instance;
    private CassandraFacade facade;
    
    public static IFileSystem getInstance() throws TTransportException,
            IOException {
        if (instance == null) {
            synchronized (CassandraFileSystem.class) {
                if (instance == null) {
                    instance = new CassandraFileSystem();
                }
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
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        String parent = PathUtil.getParent(path);
        if (!existDir(parent)) {
            mkdir(parent);
        }

        Map<String, Map<String, byte[]>> fileMap = new HashMap<String, Map<String, byte[]>>();
        Map<String, byte[]> firstRowMap = new HashMap<String, byte[]>();
        for (int i = 0; i < content.length / FSConstants.BlockSize + 1; ++i) {
            int from = i * FSConstants.BlockSize;
            int to = ((i + 1) * FSConstants.BlockSize > content.length) ? content.length
                    : (i + 1) * FSConstants.BlockSize;
            if (i == 0) {
                firstRowMap.put(/*FSConstants.FileCF + ":"
                        + */FSConstants.ContentAttr, Arrays.copyOfRange(content,
                        from, to));
            } else {
                Map<String, byte[]> chunkRow = new HashMap<String, byte[]>();
                chunkRow.put(/*FSConstants.FileCF + ":"
                        +*/ FSConstants.ContentAttr, Arrays.copyOfRange(content,
                        from, to));
                fileMap.put(path + "_$" + i, chunkRow);
            }
        }

      //  Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
        firstRowMap.put(FSConstants.TypeAttr, Bytes.toBytes("File"));
        firstRowMap.put(FSConstants.LengthAttr, Bytes.toBytes(content.length));
        firstRowMap.put(FSConstants.LastModifyTime, Bytes.toBytes(format.format(new Date())));
        firstRowMap.put(FSConstants.OwnerAttr, FSConstants.DefaultOwner);
        firstRowMap.put(FSConstants.GroupAttr, FSConstants.DefaultGroup);
        fileMap.put(path, firstRowMap);

        facade.batchPutMultipleSimpleKeyColumns(fileMap, FSConstants.FileCF);

        // add meta data for parent, except the Content
        firstRowMap.clear();
        firstRowMap.put(FSConstants.TypeAttr, Bytes.toBytes("File"));
        firstRowMap.put(FSConstants.LengthAttr, Bytes.toBytes(content.length));
        firstRowMap.put(FSConstants.LastModifyTime, Bytes.toBytes(format.format(new Date())));
        firstRowMap.put(FSConstants.OwnerAttr, FSConstants.DefaultOwner);
        firstRowMap.put(FSConstants.GroupAttr, FSConstants.DefaultGroup);

        facade.batchPut(parent, FSConstants.FolderCF, path, firstRowMap, true);
    }

    @Override
    public void createFile(String path, InputStream in) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        String parent = PathUtil.getParent(path);
        if (!existDir(parent)) {
            mkdir(parent);
        }

        boolean ensureAtomicity = false;
        Map<String, Map<String, byte[]>> keyMap = new HashMap<String, Map<String, byte[]>>();
        Map<String, byte[]> map = new HashMap<String, byte[]>();
        int length = 0;
        int index = 0;
        int num = 0;
        while (true) {
            byte[] buffer = new byte[FSConstants.BlockSize];
            num = in.read(buffer);
            LOGGER.debug("Read number of bytes: " + num + " from: " + path);
            if (num == -1) {
                break;
            }
            byte[] content;
            if(num == FSConstants.BlockSize)
                content = buffer;
            else
            {
                content = new byte[num];
                System.arraycopy(buffer, 0, content, 0, num);
            }
            
            length += num;
            if (index == 0) {
                LOGGER.debug("Putting path: " + path + ", column: " + FSConstants.FileCF + ":"
                        + FSConstants.ContentAttr );//+ ", content: " + new String(content));
                if(ensureAtomicity)
                    map.put(FSConstants.ContentAttr, content);
                else
                    facade.put(path, FSConstants.FileCF + ":"
                        + FSConstants.ContentAttr, content);
            } else {
                
                LOGGER.debug("Putting path: " + path + "_$" + index + ", column: " + FSConstants.FileCF + ":"
                        + FSConstants.ContentAttr);
                if(ensureAtomicity)
                {
                    Map<String, byte[]> chunkRow = new HashMap<String, byte[]>();
                    chunkRow.put(FSConstants.ContentAttr, content);
                    keyMap.put(path + "_$" + index, chunkRow);
                }
                else
                    facade.put(path + "_$" + index, FSConstants.FileCF + ":"
                        + FSConstants.ContentAttr, content);
            }
            index++;
        }
        LOGGER.debug("Putting META data to the queue to be inserted.");

        LOGGER.debug("Byte length: " + length + ", " + Bytes.toBytes(length));
        
        if(ensureAtomicity)
            map.clear();

        map.put(FSConstants.TypeAttr, Bytes.toBytes("File"));
        map.put(FSConstants.LengthAttr, Bytes.toBytes(length));
        map.put(FSConstants.LastModifyTime, Bytes.toBytes(format.format(new Date())));
        map.put(FSConstants.OwnerAttr, FSConstants.DefaultOwner);
        map.put(FSConstants.GroupAttr, FSConstants.DefaultGroup);
        keyMap.put(path, map);

        facade.batchPutMultipleSimpleKeyColumns(keyMap, FSConstants.FileCF);
        LOGGER.debug("Meta data inserted successfully to owner.");

        // add meta data for parent, except the Content
        map.clear();
        map.put(FSConstants.TypeAttr, Bytes.toBytes("File"));
        map.put(FSConstants.LengthAttr, Bytes.toBytes(length));
        map.put(FSConstants.LastModifyTime, Bytes.toBytes(format.format(new Date())));
        map.put(FSConstants.OwnerAttr, FSConstants.DefaultOwner);
        map.put(FSConstants.GroupAttr, FSConstants.DefaultGroup);

        facade.batchPut(parent, FSConstants.FolderCF, path, map, true);
        LOGGER.debug("Meta data inserted successfully to parent.");
    }

    public boolean deleteFile(String path) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        if (!existFile(path)) {
            LOGGER.warn("File '" + path
                    + "' can not been deleted, because it doesn't exist");
            return false;
        }
        String parent = PathUtil.getParent(path);
        facade.delete(path);
        for (int i = 1; facade.exist(path + "_$" + i); ++i) {
            facade.delete(path + "_$" + i);         ////////fix it here
        }
        facade.delete(parent, FSConstants.FolderCF, path);
        return true;
    }

    public boolean deleteDir(String path, boolean recursive) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        if (!exist(path)) {
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
                String parent = PathUtil.getParent(path);
                facade.delete(path);
                facade.delete(parent, FSConstants.FolderCF, path);
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
        String parent = PathUtil.getParent(path);
        if (parent != null && !existDir(parent)) {
            mkdir(parent);
        }

        facade.put(path, FSConstants.FolderCF + ":" + FSConstants.FolderFlag
                + ":" + FSConstants.TypeAttr, Bytes.toBytes("Dummy"));
        if (parent != null) {
            Map<String, byte[]> map = new HashMap<String, byte[]>();
            map.put(FSConstants.TypeAttr, Bytes.toBytes("Folder"));
            map.put(FSConstants.LengthAttr, Bytes.toBytes(0));
            map.put(FSConstants.LastModifyTime, Bytes.toBytes(format.format(new Date())));
            map.put(FSConstants.OwnerAttr,
                    FSConstants.DefaultOwner);
            map.put(FSConstants.GroupAttr,
                    FSConstants.DefaultGroup);

            facade.batchPut(parent, FSConstants.FolderCF, path, map, true);
        }
        LOGGER.debug("Create dir '" + path + "' succesfully");
        return true;
    }

    public List<Path> list(String path) throws IOException {
        PathUtil.checkPath(path);
        List<Path> result = new ArrayList<Path>();
        path = PathUtil.normalizePath(path);
        if (existDir(path)) {
            result = facade.list(path, FSConstants.FolderCF, false);
        } else if (existFile(path)) {
            result = facade.list(path, FSConstants.FileCF, false);
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

    public List<Path> listAll(String path) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        return facade.list(path, FSConstants.FolderCF, true);
    }

    public boolean existDir(String path) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        return facade.existDir(path);//facade.list(path, FSConstants.FolderCF, true).size() != 0;
    }

    public boolean existFile(String path) throws IOException {
        PathUtil.checkPath(path);
        path = PathUtil.normalizePath(path);
        return facade.exist(path);
    }

    public boolean exist(String path) throws IOException {
        return existDir(path) || existFile(path);
    }

    public int countDirChilden(String path) throws IOException
    {
        return facade.countRowSuper(path, FSConstants.FolderCF, StringSerializer.get());
    }

    public void copyFileToLocal(String cassFileName, String localDestFile)
    {
        FileOutputStream os = null;
        try {
            os = new FileOutputStream(localDestFile);

            

            os.close();
        } catch (IOException ex) {
            LOGGER.debug(ex);
        } finally {
            try {
                os.close();
            } catch (IOException ex) {
                LOGGER.debug(ex);
            }
        }
    }

    public static void main(String[] args) throws IOException,
            TTransportException {
        System.setProperty("storage-config", "conf");
        IFileSystem fs = CassandraFileSystem.getInstance();
        //List<Path> children = fs.list("/data");
        //List<Path> files = new ArrayList<Path>();
        fs.createFile("/usr/ftylitak/test2.cpp", Bytes.toBytes("this is a freaking test of a new file"));
        fs.readFile("/usr/ftylitak/test2.cpp");

        // fs.mkdir("/data");
        // System.out.println(fs.exist("/data"));
        //
        // fs.deleteDir("/data",true);
        // System.out.println(fs.exist("/data"));
        // fs.createFile("/data/1.txt", Bytes.toBytes("hello world3"));
        // System.out.println(fs.exist("/data"));
        // System.out.println(fs.exist("/data/a.txt"));
        // fs.deleteFile("/data/1.txt");
        // System.out.println(fs.exist("/data/a.txt"));

    }
}
