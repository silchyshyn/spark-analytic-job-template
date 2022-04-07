FROM openjdk:8-jre-alpine3.9

# copy the packaged jar file into our docker image
COPY DataProducer.jar /DataProducer.jar

COPY raw-data /raw-data

ENV BOOTSTRAP_SERVERS=localhost:9092
ENV BATCH_INTERVAL=100.millisecond
ENV BATCH_SIZE=100
ENV ORDERS_SOURCE_PATH=/raw-data/orders.json
ENV ORDERS_TOPIC=orders
ENV PRODUCTS_SOURCE_PATH=/raw-data/products.json
ENV PRODUCTS_TOPIC=products

CMD ["java", "-jar", "/DataProducer.jar", "com.trendyol.dataeng.bootcamp.DataProducer"]