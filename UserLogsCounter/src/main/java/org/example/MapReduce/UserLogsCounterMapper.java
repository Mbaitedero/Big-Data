package org.example.MapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserLogsCounterMapper extends Mapper<Object, Text, Text, IntWritable> {
    private  final static IntWritable one = new IntWritable(1);
    private Text username = new Text();

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        //chaque ligne de  log
        String line = value.toString();
        String[] tokens = line.split("\\s+"); // Un ou plusieurs espace

        if (tokens.length >=3) {
            String user = tokens[2];
            username.set(user);
            context.write(username, one);
        }

    }
}
