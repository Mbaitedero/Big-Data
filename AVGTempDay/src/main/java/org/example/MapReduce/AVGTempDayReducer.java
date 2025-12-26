package org.example.MapReduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class AVGTempDayReducer extends Reducer<Text, FloatWritable,Text,FloatWritable> {
    private FloatWritable output = new FloatWritable();

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException
    {
       float sum=0;
       int count=0;
       for (FloatWritable val : values) {
           sum+=val.get();
           count++;
       }
       if (count > 0) {
           float avg = sum/count;
           output.set(avg);
           context.write(key,output);
       }
    }
}

