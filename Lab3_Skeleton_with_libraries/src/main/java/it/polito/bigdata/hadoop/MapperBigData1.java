package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData1 extends Mapper<
        LongWritable,
        Text,
        Text,
        IntWritable> {

    protected void map(
            LongWritable key,
            Text value,
            Context context) throws IOException, InterruptedException {
        String[] products = value.toString().split(",");
        for (int outerIndex = 1; outerIndex < products.length; outerIndex++) {
            for (int innerIndex = outerIndex + 1; innerIndex < products.length; innerIndex++) {
                Text outKey;
                IntWritable outValue = new IntWritable(1);
                if (products[outerIndex].compareTo(products[innerIndex]) < 0) {
                    outKey = new Text(products[outerIndex] + ',' + products[innerIndex]);
                } else {
                    outKey = new Text(products[innerIndex] + ',' + products[outerIndex]);
                }
                context.write(outKey, outValue);
            }
        }
    }
}
