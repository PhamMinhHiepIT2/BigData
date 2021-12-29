# BigData
This repository purposes to host spark cluster, elasticsearch, rabbitmq, kibana

**Data Pipeline**
![Data Pipeline](docs/images/Data_Pipeline.jpg)

**Build cluster**
***1. Build Spark docker image***
```bash
docker build -t cluster-apache-spark:3.0.2 -f cluster/Dockerfile .
```
***2. Host cluster***
```bash
cd cluster
docker-compose up -d
```
**Build Docker image**
```bash
docker build -t bigdata -f docker/Dockerfile .
```

**Local testing**
```python
python3 -m main
```