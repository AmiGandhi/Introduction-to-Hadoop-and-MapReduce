package com.bigdata.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AccessLogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        try {

            int sum = 0;
            for(IntWritable val: values)
            {
                sum += val.get();
            }

            IntWritable count = new IntWritable(sum);
            context.write(key, count);

        } catch (Exception e) {
            System.out.println("Something went wrong in Reducer Task: "+e);
        }

    }
}