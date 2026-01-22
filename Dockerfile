FROM postgis/postgis:16-3.4

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY main.py .
COPY start.sh .
RUN chmod +x start.sh

ENV POSTGRES_DB=gis
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres

CMD ["./start.sh"]
