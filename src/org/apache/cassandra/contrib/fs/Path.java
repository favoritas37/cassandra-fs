package org.apache.cassandra.contrib.fs;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.contrib.fs.permission.FsPermission;
import org.apache.cassandra.contrib.fs.util.Bytes;
import org.apache.cassandra.thrift.Column;

public class Path
{
    private static SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm");
    // This value only will be used in cli for formatting
    public static int MaxSizeLength = Integer.MAX_VALUE;
    private String url = "";
    private String name = "";
    private String parentPath = "";
    private boolean isDir = false;
    private int version = 1;
    private long length = 0;
    private Date creationTime;
    private Date lastModTime;
    private FsPermission permission = FsPermission.getDefault();
    private String owner = "";
    private String group = "";
    private String uuid = "";
    // add other attributes,
    private Map<String, String> attributes = new HashMap<>();

    public Path(String url)
    {
        this(url, false);
    }

    public Path(String url, boolean isDir)
    {
        this.url = url;
        this.name = getNameFromURL(url);
        this.isDir = isDir;
    }

    private String getNameFromURL(String url)
    {
        int index = url.lastIndexOf("/");
        if (index == 0 && url.length() == 1) {
            parentPath = "";
            return "/";
        }
        else {
            parentPath = url.substring(0,index+1);
            if(!parentPath.equals("/")) // remove the last shlash
                parentPath = parentPath.substring(0, parentPath.length()-1);
            return url.substring(index + 1);
        }
    }

//    public Path(String url, List<Column> attributes)
//    {
//        this.url = url;
//        this.name = getNameFromURL(url);
//        for (Column attr : attributes) {
//            String attrName = new String(attr.name.array());
//            switch (attrName) {
//                case FSConstants.TypeAttr:
//                    String value = new String(attr.value.array());
//                    if (value.equals("File")) {
//                        this.isDir = false;
//                    }
//                    else {
//                        this.isDir = true;
//                    }
//                    break;
//                case FSConstants.LastModifiedTime:
//                    this.lastModTime = Bytes.toString(attr.value.array());
//                    break;
//                case FSConstants.OwnerAttr:
//                    this.owner = Bytes.toString(attr.value.array());
//                    break;
//                case FSConstants.GroupAttr:
//                    this.group = Bytes.toString(attr.value.array());
//                    break;
//                case FSConstants.LengthAttr:
//                    this.length = Bytes.toInt(attr.value.array());
//                    break;
//            }
//        }
//    }

    public Path(List<HColumn<String, byte[]>> attributes, String uuid)
    {
        this.uuid = uuid;
        for (HColumn attr : attributes) {
            String attrName = attr.getName().toString();
            switch (attrName) {
                case FSConstants.NameAttr:
                    name = new String((byte[])attr.getValue());
                    break;
                case FSConstants.PathAttr:
                    parentPath = new String((byte[])attr.getValue());
                    break;
                case FSConstants.TypeAttr:
                    String value = new String((byte[])attr.getValue());
                    if (value.equals("File"))
                        this.isDir = false;
                    else
                        this.isDir = true;
                    break;
                case FSConstants.LengthAttr:
                    byte[] array = (byte[])attr.getValue();
                    this.length = Bytes.toLong(array);
                    break;
                case FSConstants.CreationTimeAttr:
                    this.creationTime = new Date(Bytes.toLong((byte[])attr.getValue()));
                    break;
                case FSConstants.LastModifiedTime:
                    this.lastModTime = new Date(Bytes.toLong((byte[])attr.getValue()));
                    break;
                case FSConstants.OwnerAttr:
                    this.owner = new String((byte[])attr.getValue());
                    break;
                case FSConstants.GroupAttr:
                    this.group = new String((byte[])attr.getValue());
                    break;
                case FSConstants.PermissionsAttr:
                    //this needs to be changed in order to make permissions work.
                    this.permission = FsPermission.getDefault();
                    break;
            }
        }
        url = parentPath + (parentPath.endsWith("/") ? "" : "/") + name;
    }

    public boolean isDir()
    {
        return this.isDir;
    }

    public String getURL()
    {
        return this.url;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(isDir ? "d " : "- ");
        builder.append(String.format("%-8s", owner));
        builder.append(String.format("%-14s", group));
        builder.append(String.format("%-" + (MaxSizeLength + 2) + "d", length));
        builder.append(String.format("%16s", simpleFormat.format(lastModTime)));
        builder.append(" ").append(name);
        return builder.toString();
    }

    public String getName()
    {
        return this.name;
    }
    
    public String getParentPath()
    {
        return parentPath;
    }

    public long getLength()
    {
        return this.length;
    }

    public static void main(String[] args)
    {
        System.out.printf("%-10s", "zjf");
    }
}
