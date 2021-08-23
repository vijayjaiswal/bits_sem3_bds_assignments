package com.bits.wilp.bds.assignment1.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SalesCountryReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private static final Logger logger = LoggerFactory.getLogger(SalesCountryReducer.class);

    protected void reduce(Text t_key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException {
        Text key = (Text)t_key;
        //logger.info("key:"+key);

        double totalUnitPrice = 0;
        int count=0;
        double avgUnitPrice =0;
        Iterator valuesIt = values.iterator();
        while (valuesIt.hasNext()) {
            // replace type of value with the actual type of our value
            DoubleWritable value = (DoubleWritable) valuesIt.next();
            //logger.info("value:"+value);
            totalUnitPrice += value.get();
            count++;
        }
        if(count>0) {
            avgUnitPrice = totalUnitPrice / count;
        }
        logger.info("key:"+key+" count:"+count+" totalUnitPrice:"+totalUnitPrice+" avgUnitPrice:"+avgUnitPrice);
        context.write(key, new DoubleWritable(avgUnitPrice));

    }
}
