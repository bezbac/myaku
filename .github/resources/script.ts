import * as pl from "npm:nodejs-polars@0.18.0";
import * as Plot from "npm:@observablehq/plot";
import { JSDOM } from "npm:jsdom";

const commits = pl.readParquet("./.myaku/output/bezbac/myaku/commits.parquet");
const todos = pl.readParquet(
  "./.myaku/output/bezbac/myaku/metrics/pattern-occurences/data.parquet"
);

// Join commits with todos on the commit hash
let df = commits.join(todos, {
  leftOn: "id",
  rightOn: "commit",
});

const dateValues = [...df.getColumn("time")].map((time) => {
  const date = new Date(time * 1000);
  return date.toISOString().split("T")[0];
});

const dateColumn = pl.Series("date", dateValues);

df = df.withColumn(dateColumn);
df = df.drop("time");

const matchesValues = JSON.parse(
  (df.getColumn("matches") as pl.Series).toJSON()
).values;

df = df.drop("matches");

const countValues = matchesValues.map((data: any) => {
  return data.length;
});

const countColumn = pl.Series("count", countValues);

df = df.withColumn(countColumn);

const records = df.sort("date").toRecords();

const normalized = [
  ...records,
  // Append the current date with the last count
  {
    ...records[records.length - 1],
    date: new Date().toISOString().split("T")[0],
  },
];

const plot = Plot.plot({
  document: new JSDOM("").window.document,
  width: 900,
  x: {
    type: "time",
    label: null,
  },
  y: {
    type: "linear",
    label: null,
    grid: true,
    tickFormat: (d) => (d % 1 === 0 ? d.toFixed(0) : ""), // Ensure no ticks for decimals
  },
  marks: [
    Plot.line(normalized, {
      x: "date",
      y: "count",
      curve: "step-after",
      stroke: "steelblue",
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
