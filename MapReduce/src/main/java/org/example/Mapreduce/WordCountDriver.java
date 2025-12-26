package org.example.Mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    private HDFSConnexion Connexion;

    public WordCountDriver() throws Exception {
        Connexion = new HDFSConnexion();
    }
    public HDFSConnexion getConnexion() {
        return Connexion;
    }
    public void setConnexion(HDFSConnexion connexion) {
        Connexion = connexion;
    }

    public static void main(String[] args) throws Exception {
        String inputpath = "/user/input_wordCount";
        String outputpath = "/user/input_wordCount/output";
        WordCountDriver WCD  = new WordCountDriver();
        //Création de la configuarttion Hadoop
        Configuration conf = WCD.getConnexion().getFS().getConf();
        conf.set("mapreduce.framework.name","local");
        conf.set("mapreduce.cluster.local.dir","C:/hadoop_tmp/local");
        //System.out.println("Connecting to HDFS: (" + WCD.getConnexion().getFS().getConf().toString() + ")");
        //Création d'une  instance Job
       Job job = Job.getInstance(conf,"WordCount");
       //indiquer la classe  principale
        job.setJarByClass(WordCountDriver.class);
        //spécifier  les  classes map  et reduces
        job.setMapperClass(WordCoundMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //combiner(optionel)
        job.setCombinerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //Définir  les chemins  d'entrées
        FileInputFormat.addInputPath(job,new Path(inputpath));
        FileOutputFormat.setOutputPath(job,new Path(outputpath));
        System.exit(job.waitForCompletion(true) ?0:1) ;
    }
}
