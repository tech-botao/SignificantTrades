const influx2 = require('influxdb-v2');
const getHms = require('../helper').getHms;

class InfluxStorage {

    constructor(options) {
        this.format = 'tick';

        if (!options.influxUrl) {
            throw `Please set the influxdb url using influxURL property in config.json`;
        }

        this.options = options;
        console.log(`[storage/influx] options`, this.options);
    }

    connect() {
        console.log(`[storage/influx] connecting`);

        return new Promise((resolve, reject) => {
            try {
                this.influx = new influx2({
                    host: this.options.influxUrl,
                    protocol: this.options.influxProtocol,
                    port: this.options.influxPort,
                    token: this.options.influxToken,
                });
                resolve();
            } catch (error) {
                throw error;
            }
        })
    }

    save(chunk) {
        console.log(`[storage/influx] save`);

        if (!chunk || !chunk.length) {
            return Promise.resolve();
        }

        const data = [];

        return this.influx.write({org: "admin", bucket: "significant_trades", precision: "ms"}, chunk.map(trade => {
            const fields = {
                price: +trade[2],
                side: trade[4] > 0
            };

            if (trade[4] > 0) {
                fields.buy = +trade[3];
            }

            if (trade[4] < 1) {
                fields.sell = +trade[3];
            }

            if (+trade[5] === 1) {
                delete fields.buy;
                delete fields.sell;
                fields.liquidation = +trade[3];
            }

            return {
                measurement: "trades",
                tags: {
                    exchange: trade[0],
                    pair: this.options.pair
                },
                fields: fields,
                timestamp: parseInt(trade[1])
            }
        })).catch(err => {
            console.error(`[storage/influx] error saving data to InfluxDB\n${err.stack}`)
        });
    }

    async fetch(from, to, timeframe = 1000 * 60) {
        try {
            let ticks = await this.influx.query({org: "admin", bucket: "significant_trades", precision: "ms"}, {
                query:
                    `
from(bucket: "significant_trades")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "trades")
  |> filter(fn: (r) => r._field == "sell" or r._field == "buy" or r._field == "liquidation")
  |> aggregateWindow(every: v.windowPeriod, fn: sum)
  |> yield(name: "sum")
  
from(bucket: "significant_trades")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "trades")
  |> filter(fn: (r) => r._field == "side")
  |> aggregateWindow(every: v.windowPeriod, fn: count)
  |> yield(name: "count")
  
from(bucket: "significant_trades")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "trades")
  |> filter(fn: (r) => r._field == "price")
  |> aggregateWindow(every: v.windowPeriod, fn: median)
  |> yield(name: "median_price")
				`
            });
            return await ticks.map(tick => {
                tick.timestamp = +new Date(tick.time);

                delete tick.time;

                return tick;
            }).sort((a, b) => a.timestamp - b.timestamp);
        } catch (error) {
            console.error(`[storage/files] failed to retrieves trades between ${from} and ${to} with timeframe ${timeframe}\n\t`, error.message);
        }
    }
}

module.exports = InfluxStorage;
