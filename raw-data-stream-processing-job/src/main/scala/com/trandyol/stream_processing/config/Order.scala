package com.trandyol.stream_processing.config

case class Order(
  customer_id: String,
  location: String,
  order_date: String,
  order_id: String,
  price: Double,
  seller_id: String,
  status: String)
