import * as pl from "npm:nodejs-polars@0.18.0";
import * as Plot from "npm:@observablehq/plot";
import * as path from "https://deno.land/std@0.203.0/path/mod.ts";
import { JSDOM } from "npm:jsdom";

const cwd = Deno.cwd();

let df = pl.readParquet(
  path.join(cwd, ".myaku/output/bezbac/myaku/total-loc-over-time.parquet")
);

const dateValues = [...df.getColumn("commit_date")].map((time) => {
  const date = new Date(time * 1000);
  return date.toISOString().split("T")[0];
});

const dateColumn = pl.Series("date", dateValues);

df = df.withColumn(dateColumn);
df = df.drop("commit_date");

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
  className: "plot",
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
      y: "loc",
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

const style = plot.ownerDocument.createElement("style");
style.textContent = `
.plot {
  color: black;
}

@media (prefers-color-scheme: dark) {
  .plot { color: white; }
}
`;

plot.prepend(style);

console.log(plot.outerHTML);
