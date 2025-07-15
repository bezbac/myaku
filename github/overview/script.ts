import * as pl from "npm:nodejs-polars@0.18.0";
import * as Plot from "npm:@observablehq/plot";
import { JSDOM } from "npm:jsdom";

const df = pl.readParquet("./.myaku/output/bezbac/myaku/commits.parquet");

const dateValues = [...df.getColumn("time")].map((time) => {
  const date = new Date(time * 1000);
  return date.toISOString().split("T")[0];
});

const dateColumn = pl.Series("date", dateValues);

const records = df
  .withColumn(dateColumn)
  .groupBy("date")
  .agg(pl.col("date").count().alias("count"))
  .sort("date")
  .toRecords();

const firstDate = Math.min(
  ...records.map((r) => new Date(r.date as string).getTime())
);
const lastDate = Math.max(
  ...records.map((r) => new Date(r.date as string).getTime())
);

const days = Math.ceil((lastDate - firstDate) / (1000 * 60 * 60 * 24));

const normalized = Array.from({ length: days }, (_, i) => {
  const date = new Date(firstDate + i * 1000 * 60 * 60 * 24)
    .toISOString()
    .split("T")[0];

  const record = records.find((r) => r.date === date);

  return {
    date: new Date(date),
    count: record ? record.count : 0,
  };
});

const plot = Plot.plot({
  document: new JSDOM("").window.document,
  width: 900,
  marginLeft: 50,
  marks: [
    Plot.barY(normalized, {
      x: "date",
      y: "count",
    }),
  ],
});

plot.setAttributeNS(
  "http://www.w3.org/2000/xmlns/",
  "xmlns",
  "http://www.w3.org/2000/svg"
);
plot.setAttributeNS(
  "http://www.w3.org/2000/xmlns/",
  "xmlns:xlink",
  "http://www.w3.org/1999/xlink"
);

console.log(plot.outerHTML);
