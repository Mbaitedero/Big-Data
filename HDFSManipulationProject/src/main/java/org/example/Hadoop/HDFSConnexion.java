package org.example.Hadoop;

import  org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HDFSConnexion {
    private FileSystem fs ;

    public HDFSConnexion() throws IOException {
        Configuration conf = new Configuration();
        System.out.println(conf);
        //conf.set("fs.defaultFS","hdfs://localhost:9000");

        fs  = FileSystem.get(conf);
        System.out.println("Connected to HDFS ! :(" + fs.getUri()+")");
    }


    public FileSystem getFS() {
        return fs;
    }
}
