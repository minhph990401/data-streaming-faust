"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("cta_station_stream", value_type=Station)
out_topic = app.topic("cta_ride_stream", partitions=1)
table = app.Table(
    "cta_ride_stream",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def process_station():
    table[Station.station_id] = TransformedStation(station_id = Station.station_id,
                              station_name = Station.station_name,
                              order = Station.order,
                              line = "red" if Station.red else "blue" if Station.blue else "green")



if __name__ == "__main__":
    app.main()
