package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<LongWritable, // Input key type
        Text, // Input value type
        Text, // Output key type
        IntWritable> {// Output value type

    protected void map(LongWritable key, // Input key type
                       Text value, // Input value type
                       Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\\s+");
        String previousWord = null;
        for (String word : words) {
            String currentWord = word.toLowerCase();
            if (previousWord == null) {
                previousWord = currentWord;
                continue;
            }
            context.write(new Text(MessageFormat.format("{0} {1}", previousWord, currentWord)),
                    new IntWritable(1));
            previousWord = currentWord;
        }
    }
}
