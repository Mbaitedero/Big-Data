package org.example.MapReduce;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class AVGTempDayMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private FloatWritable output = new FloatWritable();
    private Text outputkey = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        //chaque ligne de  log
        String line = value.toString();
        String[] fields = line.split(",");

        if (fields.length == 3) {
            String date = fields[0].trim();
            String station = fields[1].trim();
            String temperature= String.valueOf(Float.parseFloat(fields[2].trim()));
            //Faisons  notre  choix  de  la  clé  du  groupement
            outputkey.set(date);
            output.set(Float.parseFloat(temperature));
            context.write(outputkey, output);

        }
    }
}