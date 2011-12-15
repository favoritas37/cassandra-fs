package org.apache.cassandra.contrib.fs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import org.apache.cassandra.contrib.fs.util.Bytes;

import org.apache.cassandra.thrift.NotFoundException;
import org.apache.log4j.Logger;

public class CFileInputStream extends InputStream {

    private static Logger LOGGER = Logger.getLogger(CFileInputStream.class);
    private InputStream curBlockStream;
    private String path;
    private CassandraFacade facade;
    private long length;
    private int numOfChunks;
    private int numOfChunksAcquired = 0;
    private LinkedList<byte[]> bufferList;
    private Semaphore sem;
    private boolean buffering;
    private String uuid = "";

    public CFileInputStream(String path, CassandraFacade facade)
            throws IOException {
        this.path = path;
        this.facade = facade;
        uuid = facade.getRowUUID(path);
        
        length = Bytes.toLong(facade.get(FSConstants.FileMetaCF, uuid, FSConstants.LengthAttr));
        LOGGER.debug("Length: " + length);
        
        //Add an extra semaphore since we removed the reading of the first chunk from this
        //constructor
        numOfChunks = (int)(length/(long)FSConstants.BlockSize) + (length%FSConstants.BlockSize == 0 ? 0 : 1);
        LOGGER.debug("Number of chunks: " + numOfChunks);
        sem = new Semaphore(numOfChunks, true);
        this.curBlockStream = new ByteArrayInputStream(new byte[0]);
        buffering = true;
        bufferList = new LinkedList();
        
        //initialize semaphore to be available
        sem.drainPermits();
        new BufferThread().start();
   }

    @Override
    public int read() throws IOException 
    {
        int next = curBlockStream.read();
        if (next != -1) {
            return next;
        } else {
            try
            {
                if((!buffering && bufferList.size() == 0) || numOfChunksAcquired == numOfChunks)
                    return -1;

                LOGGER.debug("Acquiring semaphore");
                sem.acquire();
                LOGGER.debug("Acquiring succeded");

                byte[] bytes = (byte[]) bufferList.poll();
                if(bytes == null)
                {
                    LOGGER.debug("Buffering file: " + path  + " reached <EOF>");
                    return -1;
                }
                numOfChunksAcquired++;
                curBlockStream = new ByteArrayInputStream(bytes);
                LOGGER.debug("ByteArrayInputStream length: " + bytes.length);
                return curBlockStream.read();
            } catch (InterruptedException ex) {
                LOGGER.debug(ex);
            } catch (IOException e) {
                if (e.getCause() instanceof NotFoundException) {
                    LOGGER.debug("NotFoundException");
                    return -1;
                } else {
                    throw e;
                }
            }
        }

        return -1;
    }


    class BufferThread extends Thread
    {
        private int blockId;

        BufferThread()
        {
            blockId = 0;
        }

        @Override
        public void run()
        {
            buffering = true;
            try {
                while (facade.exist(uuid + "_$" + blockId, FSConstants.FileDataCF)) {
                    if (blockId >= numOfChunks) {
                        new CleanerThread(blockId).start();
                        blockId++;
                        break;
                    }
                    LOGGER.debug("Buffering chunk: " + uuid + "_$" + blockId);
                    byte[] bytes = facade.get(FSConstants.FileDataCF, uuid + "_$" + blockId, FSConstants.ChunkAttr);
                    LOGGER.debug("Buffering chunk: " + uuid + "_$" + blockId++ + "...completed.");

                    bufferList.offer(bytes);
                    
                    LOGGER.debug("Releasing semaphore");
                    sem.release();
                    LOGGER.debug("Releasing completed ");
                }
            }  catch (IOException ex) {
                LOGGER.debug(ex);
            }
            buffering = false;

            if(sem.availablePermits() == 0 && sem.hasQueuedThreads())
                sem.release();
        }
    }

    class CleanerThread extends Thread
    {
        private int index;

        CleanerThread(int index)
        {
            this.index = index;
        }

        @Override
        public void run()
        {
            int tmpIndex = index;
            try {
                LOGGER.debug("Started cleaning unused chunks of file: " + uuid);
                while(true)
                {
                    String chunkName = uuid + "_$" + tmpIndex;
                    if(!facade.exist(chunkName, FSConstants.FileDataCF))
                    {
                        LOGGER.debug("Clean completed: " + uuid);
                        break;
                    }
                    LOGGER.debug("Cleaning chunk: " + chunkName + "...");
                    try
                    {
                        facade.delete(chunkName, FSConstants.FileDataCF, FSConstants.ChunkAttr);
                        LOGGER.debug("Cleaning chunk: " + chunkName + " succeeded");
                    }
                    catch(Exception e)
                    {
                        LOGGER.debug("failed");
                    }
                    tmpIndex++;
                }
            } catch (IOException ex) {
                LOGGER.debug("Error cleaning fragmented chunks for file: " + uuid + "_$" + (tmpIndex-1));
            }
        }
    }
}
