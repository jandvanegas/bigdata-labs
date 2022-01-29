package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData1 extends Reducer<
        Text,
        IntWritable,
        Text,
        IntWritable> {
    private TopKVector<WordCountWritable> topK;
    private final Integer K = 100;

    @Override
    protected void setup(Context context) {
        topK = new TopKVector<>(K);
    }

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context) {
        int counter = 0;
        for (IntWritable value : values) {
            counter += value.get();
        }

        WordCountWritable currentPairOfProducts = new WordCountWritable(key.toString(), counter);
        topK.updateWithNewElement(currentPairOfProducts);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (WordCountWritable productPair : topK.getLocalTopK()) {
            context.write(new Text(productPair.getWord()), new IntWritable(productPair.getCount()));
        }
    }
}
