# BigData
This repository purposes to create data pipeline 
**Data Pipeline**
![Data Pipeline](docs/images/Data_Pipeline.jpg)

**Build cluster**<br>
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

**Install requirements**
```bash
pip3 install -r requirements.txt
```

**Local testing**
```python
python3 -m main
```