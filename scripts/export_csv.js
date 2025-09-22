// scripts/export_csv.js
import fs from "node:fs/promises";
import path from "node:path";

const SRC = "data";
const OUT = "exports";
await fs.mkdir(OUT, { recursive: true });

function parseJsonl(txt) {
  if (!txt || !txt.trim()) return [];
  return txt.trim().split(/\n+/).map(l => JSON.parse(l));
}
function toCsv(rows, preferredOrder = []) {
  if (!rows.length) return "";
  // union of keys
  const keys = Array.from(
    rows.reduce((s, r) => {
      Object.keys(r).forEach(k => s.add(k));
      return s;
    }, new Set(preferredOrder))
  );
  // ensure preferred keys come first
  const hdr = [...preferredOrder, ...keys.filter(k => !preferredOrder.includes(k))];

  const esc = v => {
    if (v === null || v === undefined) return "";
    if (Array.isArray(v)) v = v.join("; ");
    else if (typeof v === "object") v = JSON.stringify(v);
    v = String(v);
    // CSV escape
    if (/[,"\n]/.test(v)) v = `"${v.replace(/"/g, '""')}"`;
    return v;
  };

  const lines = [
    hdr.join(","),
    ...rows.map(r => hdr.map(k => esc(r[k])).join(","))
  ];
  return lines.join("\n") + "\n";
}

async function exportOne(name, order=[]) {
  const p = path.join(SRC, `${name}.jsonl`);
  const txt = await fs.readFile(p, "utf8").catch(()=> "");
  const rows = parseJsonl(txt);
  const csv = toCsv(rows, order);
  await fs.writeFile(path.join(OUT, `${name}.csv`), csv, "utf8");
  console.log(`exported ${name}.csv (${rows.length} rows)`);
}

await exportOne("companies", ["brand","domain","hq_country","hq_address","musa_claim","musa_verified","country_of_origin","updated_at"]);
await exportOne("certs", ["brand","registry","cert_id","scope","country","valid_to","source_url","evidence_quote"]);
await exportOne("sites", ["brand","site_country","site_address","source_url","evidence_quote"]);
