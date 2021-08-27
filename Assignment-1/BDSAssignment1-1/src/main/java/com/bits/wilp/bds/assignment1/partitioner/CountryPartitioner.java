package com.bits.wilp.bds.assignment1.partitioner;

import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// Partitioner Class based on Country Name
public class CountryPartitioner extends Partitioner<Text, GeoSalesOrder> {

    @Override
    public int getPartition(Text key, GeoSalesOrder geoSalesOrder, int numPartitions) {
        String saleKeys[]=key.toString().split("~");
        String country=saleKeys[0].toLowerCase();
        return Math.abs((country.hashCode()) % numPartitions);
    }
}
