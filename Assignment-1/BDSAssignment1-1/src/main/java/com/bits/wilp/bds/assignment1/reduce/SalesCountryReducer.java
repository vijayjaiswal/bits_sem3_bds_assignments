package com.bits.wilp.bds.assignment1.reduce;

import java.io.IOException;
import java.util.Iterator;

import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import com.bits.wilp.bds.assignment1.entity.SalesAvgMinMaxVO;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SalesCountryReducer extends Reducer<Text, GeoSalesOrder, Text, SalesAvgMinMaxVO> {
    private static final Logger logger = LoggerFactory.getLogger(SalesCountryReducer.class);

    protected void reduce(Text t_key, Iterable<GeoSalesOrder> values, Context context ) throws IOException, InterruptedException {
        Text key = (Text)t_key;
        //logger.info("key:"+key);

        double totalUnitPrice = 0;
        int count=0;
        double avgUnitPrice =0;
        double min = 9999999999999999d;
        double max = 0;

        Iterator valuesIt = values.iterator();
        while (valuesIt.hasNext()) {
            // replace type of value with the actual type of our value
            GeoSalesOrder geoSalesOrder = (GeoSalesOrder) valuesIt.next();

            min = geoSalesOrder.getUnitsSold()<min?geoSalesOrder.getUnitsSold():min;
            max = geoSalesOrder.getUnitsSold()>max?geoSalesOrder.getUnitsSold():max;

            //logger.info("value:"+value);
            totalUnitPrice += geoSalesOrder.getUnitPrice();
            count++;
        }
        SalesAvgMinMaxVO salesAvgMinMaxVO=new SalesAvgMinMaxVO();
        if(count>0) {
            avgUnitPrice = totalUnitPrice / count;
            salesAvgMinMaxVO.setAverageUnitPrice(avgUnitPrice);
        }
        salesAvgMinMaxVO.setMinUnitSold(min);
        salesAvgMinMaxVO.setMaxUnitSold(max);

        logger.info("key:"+key+" salesAvgMinMaxVO:"+salesAvgMinMaxVO);


        context.write(key, salesAvgMinMaxVO);

    }
}
