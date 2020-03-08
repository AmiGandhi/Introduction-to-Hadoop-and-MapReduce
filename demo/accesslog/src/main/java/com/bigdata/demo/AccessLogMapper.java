package com.bigdata.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccessLogMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    //key needs to be converted to hadoop datatype
    Text word = new Text();
    //value needs to be converted to hadoop datatype
    IntWritable one =  new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {

            String line = value.toString();
            String[] tokens = line.split(" - - ");
            //set word from input java tokens
            word.set(tokens[0]);
            context.write(word,one);

        } catch (Exception e) {
            System.out.println("Something went wrong in Mapper Task: "+e);
        }

    }
}
