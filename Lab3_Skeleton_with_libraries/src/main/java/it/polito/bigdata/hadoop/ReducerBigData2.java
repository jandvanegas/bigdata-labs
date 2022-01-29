package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData2 extends Reducer<
        NullWritable,
        WordCountWritable,
        Text,
        IntWritable> {

    public static final int K = 100;

    @Override
    protected void reduce(
            NullWritable key,
            Iterable<WordCountWritable> values,
            Context context) throws IOException, InterruptedException {

        TopKVector<WordCountWritable> globalTopK = new TopKVector<>(K);

        for(WordCountWritable currentPair: values){
            globalTopK.updateWithNewElement(new WordCountWritable(currentPair));
        }

        for(WordCountWritable pair: globalTopK.getLocalTopK()) {
            context.write(new Text(pair.getWord()), new IntWritable(pair.getCount()));
        }
    }
}
