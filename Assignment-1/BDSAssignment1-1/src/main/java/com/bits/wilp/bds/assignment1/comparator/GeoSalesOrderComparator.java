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
        //System.out.println("in compare");
        GeoSalesOrder m = (GeoSalesOrder)o;
        GeoSalesOrder m2 = (GeoSalesOrder)o2;

        int compare = (int)m2.getTotalProfit().doubleValue() - (int)m.getTotalProfit().doubleValue();
        /*if (compare == 0) {
            compare = m.getOrderId().compareTo(m2.getOrderId());
        }*/

        return compare;

    }

}
