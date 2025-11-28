# Comm Aggregator - Minimal Prototype

## Start
1. Install Docker & Docker Compose.
2. In repo root: `docker-compose up --build`
3. Services:
   - Task Router: http://localhost:3000
   - Delivery: worker only (port 3001 for future admin)
   - Logging service: worker (port 3002 for future admin)
   - RabbitMQ management: http://localhost:15672 (guest/guest)
   - Mongo: mongodb://localhost:27017/commagg
   - Elasticsearch: http://localhost:9200 (if ready)

## Test
POST a message:
