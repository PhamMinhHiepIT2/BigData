FROM python:3.8

WORKDIR /bigdata

RUN apt update && apt install python3-pip -y \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN pip3 install /tmp/requirements.txt

COPY . .

CMD [ "python3", "-m", "main" ]



