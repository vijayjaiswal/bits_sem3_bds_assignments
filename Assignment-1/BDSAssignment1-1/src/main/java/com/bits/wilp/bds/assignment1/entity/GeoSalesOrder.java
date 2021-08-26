package com.bits.wilp.bds.assignment1.entity;

import com.bits.wilp.bds.assignment1.util.ApplicationUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

public class GeoSalesOrder implements Writable {
    private String index ; // index
    private String region; // region
    private String country; // country
    private String itemType; // item_type
    private String salesChannel ; // sales_channel
    private String orderPriority ; // order_priority
    private String strOrderDate ; // String order_date
    private Date orderDate ; // Date order_date
    private String orderId ; // order_id
    private String strShipDate ; // ship_date
    private Date shipDate ; // ship_date
    private Integer unitsSold ; // units_sold
    private Double unitPrice ; // unit_price
    private Double unitCost ; // unit_cost
    private Double totalRevenue ; // total_revenue
    private Double totalCost ; // total_cost
    private Double totalProfit ; // total_profit

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public String getSalesChannel() {
        return salesChannel;
    }

    public void setSalesChannel(String salesChannel) {
        this.salesChannel = salesChannel;
    }

    public String getOrderPriority() {
        return orderPriority;
    }

    public void setOrderPriority(String orderPriority) {
        this.orderPriority = orderPriority;
    }

    public String getStrOrderDate() {
        return strOrderDate;
    }

    public void setStrOrderDate(String strOrderDate) {
        this.strOrderDate = strOrderDate;
        this.orderDate= ApplicationUtils.getDateFromString(strOrderDate);
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(Date orderDate) {
        this.orderDate = orderDate;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getStrShipDate() {
        return strShipDate;
    }

    public void setStrShipDate(String strShipDate) {
        this.strShipDate = strShipDate;
        this.shipDate= ApplicationUtils.getDateFromString(strShipDate);
    }

    public Date getShipDate() {
        return shipDate;
    }

    public void setShipDate(Date shipDate) {
        this.shipDate = shipDate;

    }

    public Integer getUnitsSold() {
        return unitsSold;
    }

    public void setUnitsSold(Integer unitsSold) {
        this.unitsSold = unitsSold;
    }

    public Double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(Double unitPrice) {
        this.unitPrice = unitPrice;
    }

    public Double getUnitCost() {
        return unitCost;
    }

    public void setUnitCost(Double unitCost) {
        this.unitCost = unitCost;
    }

    public Double getTotalRevenue() {
        return totalRevenue;
    }

    public void setTotalRevenue(Double totalRevenue) {
        this.totalRevenue = totalRevenue;
    }

    public Double getTotalCost() {
        return totalCost;
    }

    public void setTotalCost(Double totalCost) {
        this.totalCost = totalCost;
    }

    public Double getTotalProfit() {
        return totalProfit;
    }

    public void setTotalProfit(Double totalProfit) {
        this.totalProfit = totalProfit;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(country);
        dataOutput.writeUTF(index );
        dataOutput.writeUTF(itemType);
        dataOutput.writeLong(orderDate.getTime());
        dataOutput.writeUTF(orderId );
        dataOutput.writeUTF(orderPriority );
        dataOutput.writeUTF(region);
        dataOutput.writeUTF(salesChannel );
        dataOutput.writeLong(shipDate.getTime() );
        dataOutput.writeUTF(strOrderDate );
        dataOutput.writeUTF(strShipDate );
        dataOutput.writeDouble(totalCost );
        dataOutput.writeDouble(totalProfit );
        dataOutput.writeDouble(totalRevenue );
        dataOutput.writeDouble(unitCost );
        dataOutput.writeDouble(unitPrice );
        dataOutput.writeInt(unitsSold );
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        country= dataInput.readUTF();
        index = dataInput.readUTF();
        itemType= dataInput.readUTF();
        orderDate = new Date(dataInput.readLong());
        orderId = dataInput.readUTF();
        orderPriority = dataInput.readUTF();
        region= dataInput.readUTF();
        salesChannel = dataInput.readUTF();
        shipDate = new Date(dataInput.readLong());
        strOrderDate = dataInput.readUTF();
        strShipDate = dataInput.readUTF();
        totalCost = dataInput.readDouble();
        totalProfit = dataInput.readDouble();
        totalRevenue = dataInput.readDouble();
        unitCost = dataInput.readDouble();
        unitPrice = dataInput.readDouble();
        unitsSold = dataInput.readInt();


    }
}
