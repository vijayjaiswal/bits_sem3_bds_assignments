package com.bits.wilp.bds.assignment1.map;

import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import com.bits.wilp.bds.assignment1.util.ApplicationUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

// Mapper Class Key:Country~Year Value:GeoSalesOrder
public class GeoSalesTopNMapper extends Mapper<LongWritable, Text, GeoSalesOrder, NullWritable > {
    private static final Logger logger = LoggerFactory.getLogger(GeoSalesTopNMapper.class);
    private TreeMap<Double, GeoSalesOrder> topProfitData = new TreeMap<Double, GeoSalesOrder>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String strYear=context.getConfiguration().get(ApplicationUtils.INPUT_SALE_YEAR);

        //Convert the data of the input plain text file to string
        String saleData = value.toString();
        GeoSalesOrder geoSalesOrder= ApplicationUtils.createGeoSalesOrder(saleData);

        String orderYear= ApplicationUtils.getDatePartFromDate(geoSalesOrder.getOrderDate());

        // Filter for Year
        if(orderYear.equalsIgnoreCase(strYear) ){
            if(Objects.nonNull(geoSalesOrder) && Objects.nonNull(geoSalesOrder.getUnitPrice())){
                logger.info("Processing: "+geoSalesOrder.toString());
                // Getting Total Profit
                Double totalProfit=geoSalesOrder.getTotalProfit();
                topProfitData.put(totalProfit,geoSalesOrder);
                // Holding Top 10 records only - Removing least once if new higher values comes in
                if (topProfitData.size() > 10) {
                    topProfitData.remove(topProfitData.firstKey());
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Double, GeoSalesOrder> entry : topProfitData.descendingMap().entrySet()) {
            // Key: GeoSalesOrder
            context.write(entry.getValue(), NullWritable.get());
        }
    }
}
