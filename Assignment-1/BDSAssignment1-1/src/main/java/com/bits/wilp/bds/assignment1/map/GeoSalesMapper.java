package com.bits.wilp.bds.assignment1.map;

import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import com.bits.wilp.bds.assignment1.util.ApplicationUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

// Mapper Class Key:Country~Year Value:GeoSalesOrder
public class GeoSalesMapper extends Mapper<LongWritable, Text, Text, GeoSalesOrder> {
    private static final Logger logger = LoggerFactory.getLogger(GeoSalesMapper.class);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Getting ItemType from Context
        String strItemType=context.getConfiguration().get(ApplicationUtils.INPUT_ITEM_TYPE);

        //Convert the data of the input plain text file to string
        String saleData = value.toString();
        GeoSalesOrder geoSalesOrder= ApplicationUtils.createGeoSalesOrder(saleData);

        // Filter for Item Type
        if(strItemType.equalsIgnoreCase(geoSalesOrder.getItemType())){
            if(Objects.nonNull(geoSalesOrder) && Objects.nonNull(geoSalesOrder.getUnitPrice())){
                logger.info("Processing: "+geoSalesOrder.toString());
                // Getting Year for the GeoSales data
                String orderYear= ApplicationUtils.getDatePartFromDate(geoSalesOrder.getOrderDate());
                word.set(geoSalesOrder.getCountry()+"~"+orderYear);

                context.write(word, geoSalesOrder);
            }
        }
    }
}
