package com.bits.wilp.bds.assignment1.partitioner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CountryPartitioner extends Partitioner<Text, DoubleWritable> {

    @Override
    public int getPartition(Text key, DoubleWritable doubleWritable, int numPartitions) {
        String saleKeys[]=key.toString().split("~");
        String country=saleKeys[0].toLowerCase();

        return Math.abs((country.hashCode()) % numPartitions);
    }


}
