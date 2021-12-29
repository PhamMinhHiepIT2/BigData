FROM python:3.8

WORKDIR /bigdata

RUN apt update && apt install python3-pip -y \
    && pip3 install --upgrade pip \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN pip3 install -r /tmp/requirements.txt

COPY . .

CMD [ "python3", "-m", "main" ]



