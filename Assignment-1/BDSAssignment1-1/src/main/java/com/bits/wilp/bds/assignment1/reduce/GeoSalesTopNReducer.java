package com.bits.wilp.bds.assignment1.reduce;

import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
// Reducer Class for Top 10 Orders based on Total Profit
// Key:totalProfit Value: GeoSalesOrder
public class GeoSalesTopNReducer extends Reducer<GeoSalesOrder, NullWritable, DoubleWritable, GeoSalesOrder> {
    private static final Logger logger = LoggerFactory.getLogger(GeoSalesTopNReducer.class);
    private TreeMap<DoubleWritable, GeoSalesOrder> topProfitData = new TreeMap<DoubleWritable, GeoSalesOrder>();

    protected void reduce(GeoSalesOrder key, Iterable<NullWritable> values, Context context ) throws IOException, InterruptedException {
        topProfitData.put(new DoubleWritable(key.getTotalProfit()), key);
        // Holding Top 10 Profitable Order only
        if (topProfitData.size() > 10) {
            topProfitData.remove(topProfitData.firstKey());
        }
        // Creating Unique Set for Orders
        Set<GeoSalesOrder> tmpSet=new HashSet<GeoSalesOrder>();
        for ( GeoSalesOrder entry : topProfitData.descendingMap().values()) {
            tmpSet.add(entry);
        }
        // Emitting the Output
        for ( GeoSalesOrder entry : tmpSet) {
            context.write(new DoubleWritable(entry.getTotalProfit()), entry);
        }
    }
}
