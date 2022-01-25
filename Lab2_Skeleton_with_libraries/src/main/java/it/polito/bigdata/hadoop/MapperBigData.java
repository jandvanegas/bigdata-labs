package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static it.polito.bigdata.hadoop.DriverBigData.FILTERING_WORD_KEY;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        IntWritable> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
        String filteringWord = context.getConfiguration().get(FILTERING_WORD_KEY);

        String[] words = value.toString().split("\\s+");
        for (String word : words) {
            String lowerWord = word.toLowerCase();
            if (word.matches("[a-zA-Z0-9]*") && word.startsWith(filteringWord)) {
                context.write(new Text(lowerWord), new IntWritable(1));
                context.getCounter(DriverBigData.COUNTERS.FILTER_MATCHES).increment(1);
            }
            else {
                context.getCounter(DriverBigData.COUNTERS.FILTER_NO_MATCHES).increment(1);
            }
        }
    }
}
