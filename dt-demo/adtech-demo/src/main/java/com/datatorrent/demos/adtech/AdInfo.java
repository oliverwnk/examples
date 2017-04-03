/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.adtech;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent.Aggregator;

import com.datatorrent.lib.appdata.schemas.TimeBucket;

/**
 * <p>
 * AdInfo class.
 * </p>
 *
 * @since 0.3.2
 */
public class AdInfo implements Serializable
{

  public String publisher;
  public int publisherID;
  public String advertiser;
  public int advertiserID;
  public String location;
  public int locationID;
  public double cost;
  public double revenue;
  public long impressions;
  public long clicks;
  public long time;

  public AdInfo()
  {
  }

  public AdInfo(String publisher, String advertiser, String location, double cost, double revenue, long impressions,
      long clicks, long time)
  {
    this.publisher = publisher;
    this.advertiser = advertiser;
    this.location = location;
    this.cost = cost;
    this.revenue = revenue;
    this.impressions = impressions;
    this.clicks = clicks;
    this.time = time;
  }

  public int getPublisherID()
  {
    return publisherID;
  }

  public void setPublisherID(int publisherID)
  {
    this.publisherID = publisherID;
  }

  public int getAdvertiserID()
  {
    return advertiserID;
  }

  public void setAdvertiserID(int advertiserID)
  {
    this.advertiserID = advertiserID;
  }

  public int getLocationID()
  {
    return locationID;
  }

  public void setLocationID(int locationID)
  {
    this.locationID = locationID;
  }

  /**
   * @return the publisher
   */
  public String getPublisher()
  {
    return publisher;
  }

  /**
   * @param publisher the publisher to set
   */
  public void setPublisher(String publisher)
  {
    this.publisher = publisher;
  }

  /**
   * @return the advertiser
   */
  public String getAdvertiser()
  {
    return advertiser;
  }

  /**
   * @param advertiser the advertiser to set
   */
  public void setAdvertiser(String advertiser)
  {
    this.advertiser = advertiser;
  }

  /**
   * @return the location
   */
  public String getLocation()
  {
    return location;
  }

  /**
   * @param location the location to set
   */
  public void setLocation(String location)
  {
    this.location = location;
  }

  /**
   * @return the cost
   */
  public double getCost()
  {
    return cost;
  }

  /**
   * @param cost the cost to set
   */
  public void setCost(double cost)
  {
    this.cost = cost;
  }

  /**
   * @return the revenue
   */
  public double getRevenue()
  {
    return revenue;
  }

  /**
   * @param revenue the revenue to set
   */
  public void setRevenue(double revenue)
  {
    this.revenue = revenue;
  }

  /**
   * @return the impressions
   */
  public long getImpressions()
  {
    return impressions;
  }

  /**
   * @param impressions the impressions to set
   */
  public void setImpressions(long impressions)
  {
    this.impressions = impressions;
  }

  /**
   * @return the clicks
   */
  public long getClicks()
  {
    return clicks;
  }

  /**
   * @param clicks the clicks to set
   */
  public void setClicks(long clicks)
  {
    this.clicks = clicks;
  }

  /**
   * @return the time
   */
  public long getTime()
  {
    return time;
  }

  /**
   * @param time the time to set
   */
  public void setTime(long time)
  {
    this.time = time;
  }

  @Override
  public String toString()
  {
    return "AdInfo{" +
      "publisher='" + publisher + '\'' +
      ", publisherID=" + publisherID +
      ", advertiser='" + advertiser + '\'' +
      ", advertiserID=" + advertiserID +
      ", location='" + location + '\'' +
      ", locationID=" + locationID +
      ", cost=" + cost +
      ", revenue=" + revenue +
      ", impressions=" + impressions +
      ", clicks=" + clicks +
      ", time=" + time +
      '}';
  }
  //private static final long serialVersionUID = 201505250652L;
}
