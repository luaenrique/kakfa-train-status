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


#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# I have used lines.py as a base to define it
topic = app.topic("stations_connstations", value_type=Station)
out_topic = app.topic("faust_stations_conn_table", partitions=1)
table = app.Table(
    "faust_stations_conn_table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def process_stations(stations):

    async for station in stations:
        if station.blue:
            l = 'blue'
        elif station.red:
            l = 'red'
        elif station.green:
            l = 'green'
        else:
            l = ''

        table[station.station_id] = TransformedStation(
            line=l,
            order=station.order,
            station_id=station.station_id,
            station_name=station.station_name             
        )


if __name__ == "__main__":
    app.main()
