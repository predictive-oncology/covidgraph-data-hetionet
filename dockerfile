FROM python:3.6

RUN mkdir -p /app/dataset
RUN mkdir -p /app/dataloader
RUN cd /app/dataset && \
    git clone https://github.com/dhimmel/drugbank.git

COPY dataset/*-pmids.tsv.gz.part* /app/dataset/
COPY dataset/disease-anatomy-dataset.csv /app/dataset/

RUN cat /app/dataset/disease-pmids.tsv.gz.part* > /app/dataset/disease-pmids.tsv.gz && \
    cat /app/dataset/anatomy-pmids.tsv.gz.part* > /app/dataset/anatomy-pmids.tsv.gz && \
    rm -f /app/dataset/*.gz.part*

WORKDIR /app/dataloader

COPY requirement.txt ./
RUN pip install --no-cache-dir -r requirement.txt
COPY dataloader .
COPY dataset/*.gz /app/dataset/
COPY dataset/disease-anatomy-dataset.csv /app/dataset/

CMD [ "python3", "./main.py" ]
