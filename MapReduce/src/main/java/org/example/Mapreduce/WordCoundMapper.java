package org.example.Mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;
public class WordCoundMapper extends Mapper<Object,Text, Text, IntWritable> {
    private  final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word,one);
        }
        //Version II
        /*
        String[] words = word.toString().split(" ");
        for (String word1 : words) {
            if (word1.length() > 1) {
                Text outputkey = new Text(word1.toLowerCase().trim());
                IntWritable outputvalue = new IntWritable(1);
                context.write(outputkey,outputvalue);
            }
        }*/
    }
}
