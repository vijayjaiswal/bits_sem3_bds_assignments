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

public class GeoItemTypeSalesMapper extends Mapper<LongWritable, Text, Text, GeoSalesOrder> {
    private static final Logger logger = LoggerFactory.getLogger(GeoItemTypeSalesMapper.class);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String strItemType = context.getConfiguration().get(ApplicationUtils.INPUT_ITEM_TYPE);
        String strCountry = context.getConfiguration().get(ApplicationUtils.INPUT_COUNTRY);

        //Convert the data of the input plain text file to string
        String saleData = value.toString();
        //logger.info("data: "+saleData);
        GeoSalesOrder geoSalesOrder = ApplicationUtils.createGeoSalesOrder(saleData);
        //logger.info("Processing: "+geoSalesOrder.toString());

        String orderYear = ApplicationUtils.getDatePartFromDate(geoSalesOrder.getOrderDate());

        // Filter for Item Type and Year
        if (strItemType.equalsIgnoreCase(geoSalesOrder.getItemType()) && strCountry.equalsIgnoreCase(geoSalesOrder.getCountry())) {
            if (Objects.nonNull(geoSalesOrder) && Objects.nonNull(geoSalesOrder.getUnitsSold())) {
                logger.info("Processing: " + geoSalesOrder.toString());
                word.set(geoSalesOrder.getCountry() + "~" + geoSalesOrder.getItemType()+"~"+orderYear);
                context.write(word, geoSalesOrder);
            }
        }

    }


}
