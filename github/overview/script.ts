import * as pl from "npm:nodejs-polars@0.18.0";
import * as Plot from "npm:@observablehq/plot";
import { JSDOM } from "npm:jsdom";

const df = pl.readParquet("./.myaku/output/bezbac/myaku/commits.parquet");

const dateColumn = df.getColumn("time").cast(pl.Date);
const dfWithDate = df.withColumn(dateColumn);

// TODO: Properly group by date
const groupedDf = dfWithDate.groupBy("date");

const records = df.toRecords();

const plot = Plot.plot({
  document: new JSDOM("").window.document,
  width: 900,
  marginLeft: 50,
  marks: [Plot.barY(records, Plot.groupX())],
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

process.stdout.write(plot.outerHTML);
