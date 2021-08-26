package com.bits.wilp.bds.assignment1.reduce;

import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import com.bits.wilp.bds.assignment1.entity.SalesUnitsSoldVO;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class SalesItemTypeCountryReducer extends Reducer<Text, GeoSalesOrder, Text, SalesUnitsSoldVO> {
    private static final Logger logger = LoggerFactory.getLogger(SalesItemTypeCountryReducer.class);

    protected void reduce(Text t_key, Iterable<GeoSalesOrder> values, Context context) throws IOException, InterruptedException {
        Text key = (Text) t_key;
        int totalUnitsSold = 0;

        Iterator valuesIt = values.iterator();
        while (valuesIt.hasNext()) {
            GeoSalesOrder geoSalesOrder = (GeoSalesOrder) valuesIt.next();
            totalUnitsSold += geoSalesOrder.getUnitsSold();
        }
        SalesUnitsSoldVO salesUnitsSoldVO = new SalesUnitsSoldVO();
        salesUnitsSoldVO.setTotalUnitsSold(totalUnitsSold);
        logger.info("key:" + key + " salesUnitsSoldVO:" + salesUnitsSoldVO);


        context.write(key, salesUnitsSoldVO);

    }
}
