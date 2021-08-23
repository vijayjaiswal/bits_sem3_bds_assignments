package com.vijay.spring.hadoop.map;

import com.vijay.spring.hadoop.reduce.SalesCountryReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Logger logger = LoggerFactory.getLogger(SalesMapper.class);
    private final static IntWritable one = new IntWritable(1);

    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] singleCountryData = valueString.split(",");
/*
        for (String countryDate : singleCountryData) {
            word.set(countryDate);
            context.write(word, one);
        }*/
        //Country name is on  column 7
        logger.info("============> "+singleCountryData[7]);
        word.set(singleCountryData[7]);
        context.write(word, one);

    }
}
