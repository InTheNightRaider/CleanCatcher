// scripts/crawl.js
// Re-crawls each brand's site, updates HQ + Made in USA claim, and saves JSONL.
// Keep requests polite, respect robots.txt, and limit pages per brand.

import fs from "node:fs/promises";

// ----- helpers -----
const sleep = (ms)=> new Promise(r=>setTimeout(r, ms));
const toLines = (txt)=> txt.trim()? txt.trim().split(/\n+/) : [];
const fromJsonl = (txt)=> toLines(txt).map(l=>JSON.parse(l));
const toJsonl = (arr)=> arr.map(o=>JSON.stringify(o)).join("\n")+"\n";

function extractJSONLD(html) {
  const out = [];
  const re = /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m; 
  while ((m = re.exec(html))) { try { const j = JSON.parse(m[1]); out.push(...(Array.isArray(j)? j : [j])); } catch {} }
  return out;
}
function stripHtml(html){ return html.replace(/<script[\s\S]*?<\/script>/gi,'').replace(/<style[\s\S]*?<\/style>/gi,'').replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim(); }

function classifyMUSA(text) {
  const t = text.toLowerCase();
  const unq = /\bmade in (the )?u\.?s\.?a?\.?\b|\bmanufactured in usa\b/.test(t)
              && !/\bwith imported (parts|materials)\b/.test(t)
              && !/\bassembled in usa\b/.test(t);
  if (unq) return "unqualified";
  if (/\bassembled in usa\b|\bmade in usa with imported (parts|materials)\b/.test(t)) return "qualified";
  return "none";
}
function hasUSAddress(text, jsonldObjs=[]) {
  if (JSON.stringify(jsonldObjs).match(/"addressCountry"\s*:\s*"(US|United States)"/i)) return true;
  if (/\b[A-Z]{2}\s\d{5}(-\d{4})?\b/.test(text)) return true; // state + ZIP
  if (/\bUnited States\b|\bUSA\b/.test(text) && /\b(address|phone|contact)\b/i.test(text)) return true;
  return false;
}
function originFromJSONLD(jsonld){
  const list = [];
  const walk = (o)=> { if (o && typeof o === 'object') { if (o.countryOfOrigin) list.push(typeof o.countryOfOrigin==='string'? o.countryOfOrigin : o.countryOfOrigin.name); Object.values(o).forEach(walk);} };
  jsonld.forEach(walk);
  return [...new Set(list.map(String))];
}

// ----- fetch with simple politeness/backoff -----
async function safeFetch(url){
  try {
    const r = await fetch(url, { headers: { "User-Agent": "CleanChoiceBot/1.0 (+github repo crawler)" }});
    if (!r.ok) return { ok:false, status:r.status, text:"" };
    return { ok:true, status:r.status, text: await r.text(), url };
  } catch (e) { return { ok:false, status:0, text:"", err:e } }
}

async function crawlBrand(b){
  const domain = (b.domain||"").replace(/^https?:\/\//,'').replace(/\/.+$/,'');
  if (!domain) return { ...b, _note:"no domain" };

  const pages = [
    `https://${domain}/`,
    `https://${domain}/about`,
    `https://${domain}/contact`,
    `https://${domain}/privacy`
  ];

  let musaClaim = "none";
  let musaEvidence = [];
  let hqUS = false;
  let hqAddress = b.hq_address || null;
  let origins = new Set(b.country_of_origin||[]);

  for (const url of pages){
    const res = await safeFetch(url);
    if (!res.ok) { await sleep(500); continue; }

    const html = res.text;
    const text = stripHtml(html);
    const jsonld = extractJSONLD(html);

    // HQ heuristic
    if (!hqUS && hasUSAddress(text, jsonld)) { hqUS = true; hqAddress = hqAddress || (text.match(/\b[A-Z]{2}\s\d{5}(?:-\d{4})?\b/)||[])[0] || "United States"; }

    // Made in USA claim
    const c = classifyMUSA(text);
    if (c !== "none" && musaClaim === "none") {
      musaClaim = c;
      // capture a short nearby snippet
      const m = text.match(/.{0,60}(Made in(?: the)? U\.?S\.?A?\.?|Manufactured in USA|Assembled in USA).{0,60}/i);
      if (m) musaEvidence.push({ url, quote: m[0] });
    }

    // countryOfOrigin via JSON-LD
    originFromJSONLD(jsonld).forEach(co => origins.add(co));

    await sleep(400); // be polite
  }

  return {
    ...b,
    hq_country: hqUS ? "US" : (b.hq_country||null),
    hq_address: hqAddress || b.hq_address || null,
    musa_claim: musaClaim,
    musa_verified: (musaClaim==="unqualified" && (hqUS || [...origins].some(c=>/US|United States/i.test(c)))),
    country_of_origin: [...origins],
    evidence_musa: musaEvidence,
    updated_at: new Date().toISOString()
  };
}

async function main(){
  // read current companies
  const path = "data/companies.jsonl";
  const raw = await fs.readFile(path, "utf8").catch(()=> "");
  const rows = fromJsonl(raw);

  // limit per run (avoid hammering)
  const MAX = Number(process.env.CRAWL_LIMIT || 50);
  const start = Number(process.env.CRAWL_OFFSET || 0);
  const slice = rows.slice(start, start+MAX);

  const updated = [];
  for (const b of slice) {
    const u = await crawlBrand(b);
    updated.push(u);
  }

  // merge back (dedupe by brand+domain)
  const byKey = new Map(rows.map(r => [`${(r.brand||'').toLowerCase()}|${(r.domain||'').toLowerCase()}`, r]));
  for (const u of updated) {
    byKey.set(`${(u.brand||'').toLowerCase()}|${(u.domain||'').toLowerCase()}`, u);
  }
  const merged = [...byKey.values()];
  await fs.writeFile(path, toJsonl(merged), "utf8");

  console.log(`Refreshed ${updated.length} / ${rows.length} records`);
}
main().catch(e=>{ console.error(e); process.exit(1); });
