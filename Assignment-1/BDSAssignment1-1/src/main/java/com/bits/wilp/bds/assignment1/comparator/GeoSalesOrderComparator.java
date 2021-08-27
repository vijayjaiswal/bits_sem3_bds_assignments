package com.bits.wilp.bds.assignment1.comparator;

import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.util.Comparator;

public class GeoSalesOrderComparator extends WritableComparator implements Comparator {
    public GeoSalesOrderComparator(){
        super(GeoSalesOrder.class, true);
    }

    @Override
    public int compare(WritableComparable o, WritableComparable o2){
        GeoSalesOrder m = (GeoSalesOrder)o;
        GeoSalesOrder m2 = (GeoSalesOrder)o2;

        // Comparing against Total Profit
        int compare = (int)m2.getTotalProfit().doubleValue() - (int)m.getTotalProfit().doubleValue();
        //In case profit is equal then comparing with Order Id
        if (compare == 0) {
            compare = m2.getOrderId().compareTo(m.getOrderId());
        }

        return compare;
    }

}
