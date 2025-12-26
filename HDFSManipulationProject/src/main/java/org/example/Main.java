package org.example;
import org.apache.hadoop.fs.Path;
import org.example.Hadoop.*;
import java.io.IOException;
public class Main {
    public static void main(String[] args) throws Exception {
        HDFSConnexion  connexion  = new HDFSConnexion();
        connexion.getFS().close();
        HDFSCreateFolder createFolder = new HDFSCreateFolder();
        Path dirpath  =  new Path("/user") ;
        createFolder.getDirList(createFolder.getConnexion().getFS(), dirpath);
        HDFSWrite  write = new HDFSWrite();
        write.putFile_V1(write.getConnexion().getFS(),"E:/server.log","/user/folder_1/logs");
        HDFSRead read = new HDFSRead();
        read.readFile_V2(read.getConnexion().getFS(),"/user/folder_1/logs");
        HDFSDelete delete = new HDFSDelete();
        delete.deleteFile(delete.getConnexion().getFS(),"/user/folder_1/logs");
    }
}