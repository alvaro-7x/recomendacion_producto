#!/bin/bash

cub kafka-ready -b kafka:9092 1 20 || exit 1;

# Función para iniciar cada consumer en segundo plano
start_consumer() {
  kafka-console-consumer --bootstrap-server kafka:9092 --topic $1 --from-beginning & 
}

# Lista de tópicos a consumir
topics=(
  logs
  customers_dataset
  geolocation_dataset
  order_items_dataset
  order_payments_dataset
  order_reviews_dataset
  orders_dataset
  products_dataset
  sellers_dataset
  product_category_name_translation
)

# Iniciar consumidores en segundo plano para todos los tópicos
for topic in "${topics[@]}"; do
  start_consumer "$topic"
done

wait
