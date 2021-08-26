package com.bits.wilp.bds.assignment1.reduce;

import com.bits.wilp.bds.assignment1.comparator.GeoSalesOrderComparator;
import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class GeoSalesTopNReducer extends Reducer<GeoSalesOrder, NullWritable, DoubleWritable, GeoSalesOrder> {
    private static final Logger logger = LoggerFactory.getLogger(GeoSalesTopNReducer.class);
    private TreeMap<DoubleWritable, GeoSalesOrder> topProfitData = new TreeMap<DoubleWritable, GeoSalesOrder>();

    protected void reduce(GeoSalesOrder key, Iterable<NullWritable> values, Context context ) throws IOException, InterruptedException {
        //DoubleWritable t_key = (DoubleWritable)key;
        //logger.info("key:"+key);

        //for (GeoSalesOrder geoSalesOrder : values) {
            //topProfitData.put(geoSalesOrder.getTotalProfit(), new Text(geoSalesOrder.getOrderId()));
            //topProfitData.put(geoSalesOrder.getTotalProfit(), geoSalesOrder);
            //topProfitData.put(new Text(geoSalesOrder.getTotalProfit()+"~"+geoSalesOrder.getOrderId()), geoSalesOrder);
            topProfitData.put(new DoubleWritable(key.getTotalProfit()), key);
            if (topProfitData.size() > 10) {
                topProfitData.remove(topProfitData.firstKey());
            }
       // }
        //NavigableMap<Double, GeoSalesOrder> tmpMap=topProfitData.descendingMap();
        Set<GeoSalesOrder> tmpSet=new HashSet<GeoSalesOrder>();
        for ( GeoSalesOrder entry : topProfitData.descendingMap().values()) {
            tmpSet.add(entry);
        }

        for ( GeoSalesOrder entry : tmpSet) {
        //for (Text t : topProfitData.descendingMap().entrySet()) {
           // GeoSalesOrder geoSalesOrder=entry.getValue();
            //context.write(NullWritable.get(), new Text(entry.getOrderId()));
            //context.write(new DoubleWritable(entry.getTotalProfit()), new Text(entry.getOrderId()));
            context.write(new DoubleWritable(entry.getTotalProfit()), entry);
        }




    }
}
