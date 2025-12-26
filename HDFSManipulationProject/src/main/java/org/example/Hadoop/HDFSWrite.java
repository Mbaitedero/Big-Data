package org.example.Hadoop;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class HDFSWrite {
    private HDFSConnexion connexion;
    public HDFSWrite() throws IOException {
        connexion =  new HDFSConnexion();
    }
    public HDFSConnexion getConnexion() {
        return connexion;
    }
    public void setConnexion(HDFSConnexion connexion) {
        this.connexion = connexion;
    }
    public void putFile_V1(FileSystem fs, String src, String dst) throws IOException {
        FSDataOutputStream  out = fs.create(new Path(dst));
        BufferedReader br = new BufferedReader(new FileReader(src));
        String line;
        while ((line = br.readLine()) != null) {
            out.writeBytes(line + "\n");
        }
        br.close();
        out.close();
        fs.close();
        System.out.println("file upload successfully to HDFS");

    }
    public void putFile_V2(FileSystem fs, String line, String dst) throws IOException {
        FSDataOutputStream  out = fs.create(new Path(dst), true);
        out.writeBytes(line + "\n");
        out.close();
        fs.close();
        System.out.println("file upload successfully to HDFS");
    }
    public void putFile_V3(FileSystem fs,String src,String dst) throws IOException {
      FSDataOutputStream out = fs.create(
              new Path(dst), new Progressable() {
                  @Override
                  public void progress() {
                      System.out.println(".");
                  }
              }
      );
      BufferedReader br = new BufferedReader(new FileReader(src));
      String line;
      while ((line = br.readLine()) != null) {
          out.writeBytes(line + "\n");
      }
      br.close();
      out.close();
      fs.close();
      System.out.println("file upload successfully to HDFS");
    }

}
