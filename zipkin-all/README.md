## Running

```bash
./gradlew :zipkin-all:run
-PrunArgs='-zipkin.kafka.server=mysql:2181/kafka,-zipkin.kafka.topics=zipkin=1,-zipkin.store.cassandra.dest=cassandra:9042,-zipkin.web.resourcesRoot=<path
to zipkin repo>/zipkin-web/src/main/resources'
```
