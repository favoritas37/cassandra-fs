package org.apache.cassandra.contrib.fs;


import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import org.apache.log4j.Logger;

public class FSConsistencyLevelPolicy implements ConsistencyLevelPolicy
{
    private static Logger LOGGER = Logger.getLogger(FSConsistencyLevelPolicy.class);
    private HConsistencyLevel read;
    private HConsistencyLevel write;

    public FSConsistencyLevelPolicy(String readC, String writeC)
    {
        
        if(readC.compareTo("ANY") == 0)
        {
            read = HConsistencyLevel.ANY;
            LOGGER.debug("Reading Consistency Level: ANY");
        }
        else if(readC.compareTo("ONE") == 0)
        {
            read = HConsistencyLevel.ONE;
            LOGGER.debug("Reading Consistency Level: ONE");
        }
        else if(readC.compareTo("ALL") == 0)
        {
            read = HConsistencyLevel.ALL;
            LOGGER.debug("Reading Consistency Level: ALL");
        }
        else
        {
            read = HConsistencyLevel.QUORUM;
            LOGGER.debug("Reading Consistency Level: QUORUM");
        }

        if(writeC.compareTo("ONE") == 0)
        {
            write = HConsistencyLevel.ONE;
            LOGGER.debug("Writing Consistency Level: ONE");
        }
        else if(writeC.compareTo("ALL") == 0)
        {
            write = HConsistencyLevel.ALL;
            LOGGER.debug("Writing Consistency Level: ALL");
        }
        else
        {
            write = HConsistencyLevel.QUORUM;
            LOGGER.debug("Writing Consistency Level: QUORUM");
        }
    }

    public HConsistencyLevel get(OperationType op)
    {
        switch (op)
        {
            case READ:return read;
            case WRITE: return write;
            default: return HConsistencyLevel.QUORUM; //Just in Case

        }
    }

    public HConsistencyLevel get(OperationType op, String cfName)
    {
        return HConsistencyLevel.QUORUM;
    }
}
