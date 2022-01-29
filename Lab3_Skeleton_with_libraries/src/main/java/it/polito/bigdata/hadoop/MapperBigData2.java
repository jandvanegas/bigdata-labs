package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MapperBigData2 extends Mapper<
        Text,
        Text,
        NullWritable,
        WordCountWritable> {

    protected void map(
            Text key,
            Text value,
            Context context) throws IOException, InterruptedException {
        WordCountWritable wordCountWritable = new WordCountWritable(key.toString(), Integer.parseInt(value.toString()));
        context.write(NullWritable.get(), wordCountWritable);
    }
}
