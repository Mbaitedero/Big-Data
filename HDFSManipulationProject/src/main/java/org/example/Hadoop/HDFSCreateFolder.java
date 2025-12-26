package org.example.Hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSCreateFolder {

    private  HDFSConnexion connexion ;

    public HDFSCreateFolder() throws Exception {
        connexion  =  new HDFSConnexion();
    }

    public HDFSConnexion getConnexion() {
        return connexion;
    }

    public void setConnexion(HDFSConnexion connexion) {
        this.connexion = connexion;
    }

    public void createDirectory(FileSystem fs, String path) throws Exception{
        Path dirpath = new Path(path);

        if (!fs.exists(dirpath)) {
            if (fs.mkdirs(dirpath)) {
                System.out.println("Directory created" + dirpath.toString());
            }else{
                System.out.println("Failed to create directory" + dirpath.toString());
            }
        }else{
            System.out.println("Directory exists" + dirpath.toString());
       }
        fs.close();
    }
    public void  getDirList(FileSystem fs, Path path) throws Exception{
        FileStatus[]  fileStatusList = fs.listStatus(path);

        System.out.println("contents of directory : " + path.getName());

        for (FileStatus Status : fileStatusList) {
            if (Status.isDirectory()){
                System.out.println("DIR" + Status.getPath().getName());
                getDirList(fs, Status.getPath());
            } else if (Status.isFile()) {
                System.out.println("FILE" + Status.getPath().getName());

            }
        }
    }
}
