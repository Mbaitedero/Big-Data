package org.example.MapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class UserLogsCounterDriver {
    private org.example.MapReduce.HDFSConnexion Connexion;
    public UserLogsCounterDriver() throws Exception {
        Connexion = new org.example.MapReduce.HDFSConnexion();
    }
    public org.example.MapReduce.HDFSConnexion getConnexion() {
        return Connexion;
    }
    public void setConnexion(org.example.MapReduce.HDFSConnexion connexion) {
        Connexion = connexion;
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: UserLogsCounter <input path> <output path>");
            System.exit(-1);
        }

        UserLogsCounterDriver WCD  = new UserLogsCounterDriver();
        //Création de la configuarttion Hadoop
        Configuration conf = WCD.getConnexion().getFS().getConf();

        //Création d'une  instance Job
        Job job = Job.getInstance(conf,"User Logs Count");
        //indiquer la classe  principale
        job.setJarByClass(UserLogsCounterDriver.class);
        //spécifier  les  classes map  et reduces
        job.setMapperClass(UserLogsCounterMapper.class);
        job.setReducerClass(UserLogsCounterReducer.class);
        //combiner(optionel)
        job.setCombinerClass(UserLogsCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //Définir  les chemins  d'entrées
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ?0:1) ;
    }
}
