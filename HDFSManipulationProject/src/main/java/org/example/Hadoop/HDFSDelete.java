package org.example.Hadoop;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
public class HDFSDelete {
    private HDFSConnexion connexion;
    public HDFSDelete() throws IOException
    {
        connexion = new HDFSConnexion();
    }
    public HDFSConnexion getConnexion() {
        return connexion;
    }
    public void deleteFile(FileSystem fs, String pathdelete) throws IOException
    {
    Path p = new Path(pathdelete);
    if (fs.exists(p)) {
        if (fs.delete(p, true)) {
            System.out.println("Delete file " + pathdelete + " successfully");
        }else {
            System.out.println("Delete file " + pathdelete + " failed");
        }
    } else  {
        System.out.println("Delete file " + pathdelete + " does not exist");
    }
    fs.close();
   }
}
