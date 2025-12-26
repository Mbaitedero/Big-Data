package org.example.Hadoop;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.CompletableFuture;

public class HDFSRead {
    private HDFSConnexion connexion;

    public HDFSRead() throws IOException{
        connexion = new HDFSConnexion();
    }
    public HDFSConnexion getConnexion(){
        return connexion;
    }
    public void setConnexion(HDFSConnexion connexion){
        this.connexion = connexion;
    }
    public void readFile(FileSystem fs, String filepath) throws IOException{
        try{
           Path path = new Path(filepath);
           if(fs.exists(path)){
               FSDataInputStream in =fs.open(path);
               BufferedReader br = new BufferedReader(new InputStreamReader(in));
               String line;
               while((line=br.readLine())!=null){
                   System.out.println(line);
               }
               System.out.println("file read successfully from HDFS");
               br.close();
               in.close();
           }else {
               System.out.println("file does not exist");
           }
           fs.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public void readFile_V2(FileSystem fs, String filepath) throws IOException{
        try{
            Path path = new Path(filepath);
            if(fs.exists(path)){
                FutureDataInputStreamBuilder builder = fs.openFile(path).opt("BufferSize","4096")
                        .opt("fs.option.openfile.read.policy","whole-file");

                CompletableFuture<FSDataInputStream> future = builder.build();
                FSDataInputStream in = future.get();
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String line;
                while((line=br.readLine())!=null){
                    System.out.println(line);
                }
                System.out.println("file read successfully from HDFS");
                br.close();
                in.close();
            }else  {
                System.out.println("file does not exist");
            }
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        HDFSRead read = new HDFSRead();
        read.readFile_V2(read.getConnexion().getFS(),"/user/folder_1/logs");
    }
}
