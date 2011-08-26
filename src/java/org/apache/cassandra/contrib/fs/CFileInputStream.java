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
    private int blockIndex = 0;
    private String path;
    private CassandraFacade facade;
    private int length;
    private int numOfChunks;
    private LinkedList<byte[]> bufferList;
    private final Semaphore sem = new Semaphore(FSConstants.MaxFileSize / FSConstants.BlockSize, true);
 //   private final Semaphore accessSem = new Semaphore(1, true);
    private boolean buffering;

    public CFileInputStream(String path, CassandraFacade facade)
            throws IOException {
        this.path = path;
        this.facade = facade;
        byte[] bytes = facade.get(path, FSConstants.FileCF + ":"
                + FSConstants.ContentAttr);
        length = Bytes.toInt(facade.get(path, FSConstants.FileCF + ":"
                + FSConstants.LengthAttr));

        LOGGER.debug("Length: " + length);

        this.curBlockStream = new ByteArrayInputStream(bytes);
        this.blockIndex++;
        numOfChunks = length/FSConstants.BlockSize;
        buffering = false;
        bufferList = new LinkedList();

        //initialize semaphore to be available
        //accessSem.release();
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
//                LOGGER.debug("Acquiring access");
//                accessSem.acquire();
//                LOGGER.debug("Access granded");
                if(!buffering && bufferList.size() == 0)
                    return -1;
            
            
   //             accessSem.release();
                
                LOGGER.debug("Acquiring semaphore");
                sem.acquire();
                LOGGER.debug("Acquiring succeded");
                
                
                byte[] bytes = (byte[]) bufferList.poll();
                if(bytes == null)
                {
                    LOGGER.debug("Error: writing buffered chunks.");
                    LOGGER.debug("Error: NULL returned from buffering file: " + path);
                    return -1;
                }

                curBlockStream = new ByteArrayInputStream(bytes);
                return curBlockStream.read();
            } catch (InterruptedException ex) {
                LOGGER.debug(ex);
            } catch (IOException e) {
                if (e.getCause() instanceof NotFoundException) {
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
            blockId = 1;
        }

        @Override
        public void run()
        {
            buffering = true;
            try {
                while (facade.exist(path + "_$" + blockId)) {
                    if (blockId > numOfChunks) {
                        new CleanerThread(path, blockId).start();
                        blockId++;
                        break;
                    }
        //            accessSem.release();
                    LOGGER.debug("Buffering chunk: " + path + "_$" + blockId);
                    byte[] bytes = facade.get(path + "_$" + blockId, FSConstants.FileCF + ":" + FSConstants.ContentAttr);
                    LOGGER.debug("Buffering chunk: " + path + "_$" + blockId++ + "...completed.");

                    bufferList.offer(bytes);
                    
                    LOGGER.debug("Releasing semaphore");
                    sem.release();
                    LOGGER.debug("Releasing completed ");

                    //accessSem.acquire();
                }
            } /*catch (InterruptedException ex) {
                LOGGER.debug(ex);
            }*/ catch (IOException ex) {
                LOGGER.debug(ex);
            }
            buffering = false;
//            accessSem.release();

            if(sem.availablePermits() == 0 && sem.hasQueuedThreads())
                sem.release();
        }
    }

    class CleanerThread extends Thread
    {
        private String path;
        private int index;

        CleanerThread(String path, int index)
        {
            this.path = path;
            this.index = index;
        }

        @Override
        public void run()
        {
            int tmpIndex = index;
            try {
                LOGGER.debug("Started cleaning unused chunks of file: " + path);
                while(true)
                {
                    String chunkName = path + "_$" + tmpIndex;
                    if(!facade.exist(chunkName))
                    {
                        LOGGER.debug("Clean completed: " + path);
                        break;
                    }
                    LOGGER.debug("Cleaning chunk: " + chunkName + "...");
                    try
                    {
                        facade.delete(chunkName);
                        LOGGER.debug("Cleaning chunk: " + chunkName + " succeeded");
                    }
                    catch(Exception e)
                    {
                        LOGGER.debug("failed");
                    }

                    tmpIndex++;
                }
            } catch (IOException ex) {
                LOGGER.debug("Error cleaning fragmented chunks for file: " + path + "_$" + (tmpIndex-1));
            }
        }
    }
}
