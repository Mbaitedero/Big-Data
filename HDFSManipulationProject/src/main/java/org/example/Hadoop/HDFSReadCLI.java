package org.example.Hadoop;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HDFSReadCLI {
    private HDFSConnexion connexion;

    public HDFSReadCLI() throws IOException {
        connexion =new HDFSConnexion();
    }
    public HDFSConnexion getConnexion() {
        return connexion;
    }
    public  void readFile(FileSystem fs, String path) throws IOException {
        try {
            Path p = new Path(path);
            if (fs.exists(p)) {
                FSDataInputStream in = fs.open(p);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }
                br.close();
                in.close();
            }else  {
                System.out.println("file not found");
            }
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: java -jar HDFSReadCLI.jar <hdfs-file-path>");
            System.exit(1);
        }
        String path = args[0];
        HDFSReadCLI hdfs = new HDFSReadCLI();
        hdfs.readFile(hdfs.getConnexion().getFS(), path);
    }
}
