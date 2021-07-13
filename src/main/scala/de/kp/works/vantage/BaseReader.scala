package de.kp.works.vantage
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */
import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

trait BaseReader {

  protected val base = "https://www.alphavantage.co/query?"
  
  /* Time interval between two consecutive data points in the time series. */
  protected val INTERVALS = Array("1min", "5min", "15min", "30min", "60min")

  /*
   * Two years of minute-level intraday data contains over 2 million data points, 
   * which can take up to Gigabytes of memory. To ensure optimal API response speed, 
   * the trailing 2 years of intraday data is evenly divided into 24 "slices" 
   *
   * Each slice is a 30-day window, with year1month1 being the most recent and 
   * year2month12 being the farthest from today. By default, slice=year1month1.
   * 
   */
   protected val SLICES = Array(

       /*** RECENT YEAR ***/
       
       /* Covering the most recent 30 days */
       "year1month1",
       
       /* Covering the most recent day 31 through day 60 */       
       "year1month2",
       
       "year1month3",
       "year1month4",
       "year1month5",
       "year1month6",
       "year1month7",
       "year1month8",
       "year1month9",
       "year1month10",
       "year1month11",
       "year1month12",
       
       /*** FAREST YEAR ****/
       
       "year2month1",
       "year2month2",
       "year2month3",
       "year2month4",
       "year2month5",
       "year2month6",
       "year2month7",
       "year2month8",
       "year2month9",
       "year2month10",
       "year2month11",
       "year2month12"
   )
 
  protected val major_formatter = new SimpleDateFormat("yyyy-MM-dd")
  protected val minor_formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
   
  protected val baseFields = 
      Array(
          StructField("timestamp", LongType,   true),
          StructField("open",      DoubleType, true),
          StructField("high",      DoubleType, true),
          StructField("low",       DoubleType, true),
          StructField("close",     DoubleType, true))

  protected def datetime2Ts(datetime:String, method:String):Long = {
    
    val date = method match {
      case 
      "FX_INTRADAY" | 
      "TIME_SERIES_INTRADAY" | 
      "TIME_SERIES_INTRADAY_EXTENDED" => 
        minor_formatter.parse(datetime)
        
      case 
      "FX_DAILY" |
      "FX_WEEKLY" |
      "FX_MONTHLY" |
      "TIME_SERIES_DAILY" | 
      "TIME_SERIES_DAILY_ADJUSTED" | 
      "TIME_SERIES_WEEKLY" | 
      "TIME_SERIES_WEEKLY_ADJUSTED" | 
      "TIME_SERIES_MONTHLY" | 
      "TIME_SERIES_MONTHLY_ADJUSTED" => 
        major_formatter.parse(datetime)

      case _ => 
        throw new Exception(s"Method `${method}` is not supported.")
    }
    
    date.getTime

  }
    
  protected def buildSchema(method:String):StructType = {
   
    method match {
      case 
      "FX_INTRADAY" |
      "FX_DAILY" |
      "FX_WEEKLY" |
      "FX_MONTHLY" => {
        StructType(baseFields)
      }
      case 
      "TIME_SERIES_INTRADAY" |
      "TIME_SERIES_INTRADAY_EXTENDED" |
      "TIME_SERIES_DAILY" |
      "TIME_SERIES_WEEKLY" | 
      "TIME_SERIES_MONTHLY" => {
        StructType(baseFields ++ 
            Array(StructField("volume", IntegerType, true)))
      }      
      case "TIME_SERIES_DAILY_ADJUSTED" => {
        StructType(baseFields ++
          Array(
            StructField("adj_close",         DoubleType,  true),
            StructField("volume",            IntegerType, true),
            StructField("dividend_amount",   DoubleType,  true),
            StructField("split_coefficient", DoubleType,  true)))      
      }     
      case 
      "TIME_SERIES_WEEKLY_ADJUSTED" | 
      "TIME_SERIES_MONTHLY_ADJUSTED" => {
        StructType(baseFields ++ 
          Array(
            StructField("adj_close",         DoubleType,  true),
            StructField("volume",            IntegerType, true),
            StructField("dividend_amount",   DoubleType,  true)))      
      }

      case _ => 
        throw new Exception(s"Method `${method}` is not supported.")
 
    }
    
  }

  protected def insertRow(line:String, method:String): Row = {
   
    method match {
      case 
      "FX_INTRADAY" |
      "FX_DAILY" |
      "FX_WEEKLY" |
      "FX_MONTHLY" => {
  
        val tokens = line.split(",")
      
        /* Datetime & Timestamp */
    
        /* Format: 2021-04-01 19:00:00 */
        val datetime = tokens(0).trim
        val timestamp = datetime2Ts(datetime, method)
  
        val open = tokens(1).trim.toDouble
        val high = tokens(2).trim.toDouble
  
        val low = tokens(3).trim.toDouble    
        val close = tokens(4).trim.toDouble
  
        val values = Seq(timestamp, open, high, low, close)
        Row.fromSeq(values)
      }
      case 
      "TIME_SERIES_INTRADAY" |
      "TIME_SERIES_INTRADAY_EXTENDED" |
      "TIME_SERIES_DAILY" |
      "TIME_SERIES_WEEKLY" | 
      "TIME_SERIES_MONTHLY" => {
  
        val row = line.split(",")
      
        /* Datetime & Timestamp */
      
        /* Format: 2021-04-01 19:00:00 */
        val datetime = row(0).trim
        val timestamp = datetime2Ts(method, datetime)

        val open = row(1).trim.toDouble
        val high = row(2).trim.toDouble
  
        val low = row(3).trim.toDouble
        val close = row(4).trim.toDouble

        val volume = row(5).trim.toInt
  
        val values = Seq(timestamp, open, high, low, close, volume)
        Row.fromSeq(values)
        
      }        
      case "TIME_SERIES_DAILY_ADJUSTED" => {
   
        val row = line.split(",")
        
        /* Datetime & Timestamp */
      
        /* Format: 2021-04-01 19:00:00 */
        val datetime = row(0).trim
        val timestamp = datetime2Ts(method, datetime)
  
        val open = row(1).trim.toDouble
        val high = row(2).trim.toDouble
      
        val low = row(3).trim.toDouble
        val close = row(4).trim.toDouble

        val adj_close = row(5).trim.toDouble
        val volume = row(6).trim.toInt
    
        val dividend_amount = row(7).trim.toDouble
        val split_coefficient = row(8).trim.toDouble
        
        val values = Seq(timestamp, open, high, low, close, adj_close, volume, dividend_amount, split_coefficient)
        Row.fromSeq(values)
        
      }
     
      case 
      "TIME_SERIES_WEEKLY_ADJUSTED" | 
      "TIME_SERIES_MONTHLY_ADJUSTED" => {
   
        val row = line.split(",")
        
        /* Datetime & Timestamp */
      
        /* Format: 2021-04-01 19:00:00 */
        val datetime = row(0).trim
        val timestamp = datetime2Ts(method, datetime)
  
        val open = row(1).trim.toDouble
        val high = row(2).trim.toDouble
      
        val low = row(3).trim.toDouble
        val close = row(4).trim.toDouble

        val adj_close = row(5).trim.toDouble
        val volume = row(6).trim.toInt
    
        val dividend_amount = row(7).trim.toDouble
        
        val values = Seq(timestamp, open, high, low, close, adj_close, volume, dividend_amount)
        Row.fromSeq(values)

      }
        
      case _ => throw new Exception(s"Method `${method}` is not supported.")
    }
  }
  
}