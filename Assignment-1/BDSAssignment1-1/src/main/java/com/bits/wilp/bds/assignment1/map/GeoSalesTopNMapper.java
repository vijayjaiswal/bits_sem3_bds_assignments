package com.bits.wilp.bds.assignment1.map;

import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import com.bits.wilp.bds.assignment1.util.ApplicationUtils;
import org.apache.hadoop.io.DoubleWritable;
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

public class GeoSalesTopNMapper extends Mapper<LongWritable, Text, GeoSalesOrder, NullWritable > {
    private static final Logger logger = LoggerFactory.getLogger(GeoSalesTopNMapper.class);
    private TreeMap<Double, GeoSalesOrder> topProfitData = new TreeMap<Double, GeoSalesOrder>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //String strItemType=context.getConfiguration().get(ApplicationUtils.INPUT_ITEM_TYPE);
        String strYear=context.getConfiguration().get(ApplicationUtils.INPUT_SALE_YEAR);

        //Convert the data of the input plain text file to string
        String saleData = value.toString();
        //logger.info("data: "+saleData);
        GeoSalesOrder geoSalesOrder= ApplicationUtils.createGeoSalesOrder(saleData);
        //logger.info("Processing: "+geoSalesOrder.toString());

        String orderYear= ApplicationUtils.getDatePartFromDate(geoSalesOrder.getOrderDate());

        // Filter for Item Type and Year
        //logger.info("Filtering for item ("+strItemType+") and sale year("+strYear+")");
        //logger.info("data orderYear("+orderYear+") and ItemType("+geoSalesOrder.getItemType()+")");
        //if(orderYear.equalsIgnoreCase(strYear) && strItemType.equalsIgnoreCase(geoSalesOrder.getItemType())){
        if(orderYear.equalsIgnoreCase(strYear) ){
            if(Objects.nonNull(geoSalesOrder) && Objects.nonNull(geoSalesOrder.getUnitPrice())){
                logger.info("Processing: "+geoSalesOrder.toString());

                Double totalProfit=geoSalesOrder.getTotalProfit();
                topProfitData.put(totalProfit,geoSalesOrder);

                if (topProfitData.size() > 10) {
                    topProfitData.remove(topProfitData.firstKey());
                }

            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Double, GeoSalesOrder> entry : topProfitData.descendingMap().entrySet()) {
            //context.write(NullWritable.get(), entry.getValue());
            //context.write(new DoubleWritable(entry.getKey()), entry.getValue());
            context.write(entry.getValue(), NullWritable.get());
        }
    }
}
