package de.kp.works.vantage
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import org.apache.spark.sql.{DataFrame, Row}

import de.kp.works.http.HttpConnect
import de.kp.works.spark.Session

class VantageReader extends BaseReader with HttpConnect {
  
  private var apiKey:String = ""
  
  private var adjusted:String = "false"
  private var outputSize:String = "compact"
  
  private var method:String = ""
  private var numSlices:Int = 1
  
  private var interval:String = ""
  private var slice:String = ""
  
  private val datatype:String = "csv"
  
  private val session = Session.getSession
 
  def setApiKey(value:String):VantageReader = {
    apiKey = value
    this
  }
  
  def setAdjusted(value:String):VantageReader = {
    adjusted = value
    this
  }
  
  def setInterval(value:String):VantageReader = {
    interval = value
    this
  }
   
  def setMethod(value:String):VantageReader = {
    method = value
    this
  }
  
  def setOutputSize(value:String):VantageReader = {
    outputSize = value
    this
  }
  
  def setSlice(value:String):VantageReader = {
    slice = value
    this
  }
  
  def readForex(from:String, to:String):DataFrame = {

    try {
      
      val endpoint = getEndpoint(from, to)
      read(endpoint)
      
    } catch {
      case t:Throwable => {
        log.error(s"Extracting AlphaVantage response failed with: " + t.getLocalizedMessage)
        session.emptyDataFrame
      }
    }
    
  }
  
  def readSymbol(symbol:String):DataFrame = {

    try {
      
      val endpoint = getEndpoint(symbol)
      read(endpoint)
        
    } catch {
      case t:Throwable => {
        log.error(s"Extracting AlphaVantage response failed with: " + t.getLocalizedMessage)
        session.emptyDataFrame
      }
    }
  
  }
  
  private def read(endpoint:String):DataFrame = {
      
    /****************************************
     * 
     * STAGE #1: Retrieve dataset from AlphaVantage
     * as chunked CSV data
     * 
     ***************************************/

    val bytes = get(endpoint)    
    val result = extractCsvBody(bytes)
    /*
     * The header represents the first line of the comma-separated
     * HTTP response and the respective [[String]] is used to remove
     * this initial line
     */
    val data = result.tail
    if (data.length == 0) return session.emptyDataFrame
      
    /****************************************
     * 
     * STAGE #2: Transform retrieved CSV into
     * an Apache Spark DataFrame
     * 
     ***************************************/
      
    transform(data)    
    
  }
  
  private def getEndpoint(from:String, to:String):String = {
    
    method match {
      case "FX_INTRADAY" =>
        fxIntraday(from, to)
        
      case "FX_DAILY" =>
        fxDaily(from, to)
        
      case "FX_WEEKLY" =>
        fxWeekly(from, to)
        
      case "FX_MONTHLY" =>
        fxMonthly(from, to)
        
      case _ => 
        throw new Exception(s"Method `${method}` is not supported.")

    }
  }
  private def getEndpoint(symbol:String):String = {
    
    method match {
      case "TIME_SERIES_INTRADAY" => 
        intraDayTs(symbol)

      case "TIME_SERIES_INTRADAY_EXTENDED" =>
        intraDayExtendedTs(symbol)
        
      case "TIME_SERIES_DAILY" =>
        dailyTs(symbol)
        
      case "TIME_SERIES_DAILY_ADJUSTED" =>
        dailyTsAdjusted(symbol)
        
      case "TIME_SERIES_WEEKLY" =>
        weeklyTs(symbol)

      case "TIME_SERIES_WEEKLY_ADJUSTED" =>
        weeklyTsAdjusted(symbol)
        
      case "TIME_SERIES_MONTHLY" => 
        monthlyTs(symbol)
        
      case "TIME_SERIES_MONTHLY_ADJUSTED" =>
        monthlyTsAdjusted(symbol)
        
      case _ => 
        throw new Exception(s"Method `${method}` is not supported.")
        
    }
  }
  
  private def intraDayTs(symbol:String):String = {
    /*
     * By default, the query parameter `outputsize` = compact
     * 
     * Strings `compact` and `full` are accepted with the following 
     * specifications: 
     * 
     * - compact returns only the latest 100 data points in the intraday 
     *   time series; 
     *   
     * - full returns the full-length intraday time series. 
     * 
     * The `compact` option is recommended if you would like to reduce 
     * the data size of each API call.
     * 
     * By default, adjusted = true and the output time series is adjusted by 
     * historical split and dividend events. 
     * 
     * Set adjusted=false to query raw (as-traded) intraday values.
     */
    if (INTERVALS.contains(interval) == false)
      throw new Exception(s"The provided interval `${interval}` is not supported.")
    
    /*
     * CSV format: The output format is independent of `adjusted` 
     * 
     * timestamp,          open,    high,    low,     close,   volume
     * 2021-04-01 18:05:00,133.2800,133.2800,133.2300,133.2300,905
     * 
     */        
    val endpoint = s"${base}function=${method}&symbol=${symbol}&interval=${interval}&outputsize=${outputSize}&adjusted=${adjusted}&datatype=${datatype}&apikey=${apiKey}"
    endpoint
    
  }
  /*
   * This API returns historical intraday time series for the trailing 2 years, 
   * covering over 2 million data points per ticker. The intraday data is computed 
   * directly from the Securities Information Processor (SIP) market-aggregated data 
   * feed. 
   * 
   * You can query both raw (as-traded) and split/dividend-adjusted intraday data from 
   * this endpoint. 
   * 
   * Common use cases for this API include data visualization, trading simulation/backtesting,
   * and machine learning and deep learning applications with a longer horizon.
   * 
   */
  private def intraDayExtendedTs(symbol:String):String = {
    /*
     * By default, adjusted=true and the output time series is adjusted 
     * by historical split and dividend events. 
     * 
     * Set adjusted=false to query raw (as-traded) intraday values.
     */
    if (INTERVALS.contains(interval) == false)
      throw new Exception(s"The provided interval `${interval}` is not supported.")
    
    if (SLICES.contains(slice) == false)
      throw new Exception(s"The provided slice `${slice}` is not supported.")

    /*
     * CSV format: The output format is independent of `adjusted` 
     * 
     * time,              open,    high,    low,     close,   volume
     * 2021-04-01 18:05:00,133.2800,133.2800,133.2300,133.2300,905
     * 
     * IMPORTANT: 
     * 
     * The header specification differs from the intra day response
     */    

    val endpoint = s"${base}function=${method}&symbol=${symbol}&interval=${interval}&slice=${slice}&adjusted=${adjusted}&apikey=${apiKey}"    
    endpoint

  }

  private def dailyTs(symbol:String):String = {
    /*
     * TIME_SERIES_DAILY
     *
     * This API returns raw (as-traded) daily time series (date, daily open, daily high, daily low,
     * daily close, daily volume) of the global equity specified, covering 20+ years of historical 
     * data.
     *  
     * CSV format: 
     * 
     * timestamp,          open,    high,    low,     close,   volume
     * 2021-04-01 18:05:00,133.2800,133.2800,133.2300,133.2300,905
     */
    val endpoint = s"${base}function=${method}&symbol=${symbol}&outputsize=${outputSize}&datatype=${datatype}&apikey=${apiKey}"
    endpoint
    
  }

  private def dailyTsAdjusted(symbol:String):String = {
    
    adjusted = "true"
    /*
     * TIME_SERIES_DAILY_ADJUSTED
     * 
     * This API returns raw (as-traded) daily open/high/low/close/volume values, 
     * daily adjusted close values, and historical split/dividend events of the 
     * global equity specified, covering 20+ years of historical data.
     *
     * CSV format:
     * 
     * timestamp, open,  high,  low,   close, adjusted_close,volume, dividend_amount,split_coefficient
     * 2021-04-01,133.76,133.93,132.27,133.23,133.23,        4074161,0.0000,         1.0
     */
    val endpoint = s"${base}function=${method}&symbol=${symbol}&outputsize=${outputSize}&datatype=${datatype}&apikey=${apiKey}"
    endpoint
    
  }

  private def weeklyTs(symbol:String):String = {
    /*
     * This API returns weekly time series (last trading day of each week, weekly open, weekly 
     * high, weekly low, weekly close, weekly volume) of the global equity specified, covering 
     * 20+ years of historical data.
     */
    val endpoint = s"${base}function=${method}&symbol=${symbol}&outputsize=${outputSize}&datatype=${datatype}&apikey=${apiKey}"
    endpoint
    
  }

  private def weeklyTsAdjusted(symbol:String):String = {

    adjusted = "true"
    /*
     * This API returns weekly adjusted time series (last trading day of each week, weekly open, 
     * weekly high, weekly low, weekly close, weekly adjusted close, weekly volume, weekly 
     * dividend) of the global equity specified, covering 20+ years of historical data.
     */
    val endpoint = s"${base}function=${method}&symbol=${symbol}&outputsize=${outputSize}&datatype=${datatype}&apikey=${apiKey}"
    endpoint
    
  }

  def monthlyTs(symbol:String):String = {
    /*
     * This API returns monthly time series (last trading day of each month, monthly open, 
     * monthly high, monthly low, monthly close, monthly volume) of the global equity 
     * specified, covering 20+ years of historical data.
     */
    val endpoint = s"${base}function=${method}&symbol=${symbol}&outputsize=${outputSize}&datatype=${datatype}&apikey=${apiKey}"
    endpoint
    
  }

  def monthlyTsAdjusted(symbol:String):String = {

    adjusted = "true"
    /*
     * This API returns monthly adjusted time series (last trading day of each month, monthly 
     * open, monthly high, monthly low, monthly close, monthly adjusted close, monthly volume, 
     * monthly dividend) of the equity specified, covering 20+ years of historical data.
     */
    val endpoint = s"${base}function=${method}&symbol=${symbol}&outputsize=${outputSize}&datatype=${datatype}&apikey=${apiKey}"
    endpoint
    
  }

  /*
   * This API returns intraday time series (timestamp, open, high, low, close) 
   * of the FX currency pair specified, updated realtime.
   */
  def fxIntraday(from:String = "EUR",to:String = "USD"):String = {
    
    val endpoint = s"${base}function=${method}&from_symbol=${from}&to_symbol=${to}&interval=${interval}&outputsize=${outputSize}&datatype=${datatype}&apikey=${apiKey}"
    endpoint
  }
  /*
   * This API returns the daily time series (timestamp, open, high, low, close) 
   * of the FX currency pair specified, updated realtime.
   */
  def fxDaily(from:String="EUR", to:String="USD"):String = {

    val endpoint = s"${base}function=${method}&from_symbol=${from}&to_symbol=${to}&outputsize=${outputSize}&datatype=${datatype}&apikey=${apiKey}"
    endpoint
    
  }
  /*
   * This API returns the weekly time series (timestamp, open, high, low, close) 
   * of the FX currency pair specified, updated realtime.
   * 
   * The latest data point is the price information for the week (or partial week) 
   * containing the current trading day, updated realtime.
   */
  def fxWeekly(from:String="EUR", to:String="USD"):String = {

    val endpoint = s"${base}function=${method}&from_symbol=${from}&to_symbol=${to}&datatype=${datatype}&apikey=${apiKey}"
    endpoint    
    
  }
  /*
   * This API returns the monthly time series (timestamp, open, high, low, close) 
   * of the FX currency pair specified, updated realtime.
   * 
   * The latest data point is the prices information for the month (or partial month) 
   * containing the current trading day, updated realtime.
   */
  def fxMonthly(from:String="EUR", to:String="USD"):String = {
    
    val endpoint = s"${base}function=${method}&from_symbol=${from}&to_symbol=${to}&datatype=${datatype}&apikey=${apiKey}"
    endpoint
    
  }

  private def transform(data:Seq[String]):DataFrame = {

    val schema = buildSchema(method)
    val rows = data.map(line => insertRow(line, method))
      
    session.createDataFrame(session.sparkContext.parallelize(rows, numSlices), schema)
    
  }
  
}