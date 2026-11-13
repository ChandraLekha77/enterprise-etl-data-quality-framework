"""Dependency-free operational dashboard served by FastAPI."""

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

HTML = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>ETL Quality Dashboard</title>
<style>body{font:14px system-ui;margin:0;background:#f4f7fb;color:#162033}header{padding:20px 3%;background:#15233c;color:white}main{padding:20px 3%}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}.card,section{background:white;border-radius:10px;padding:16px;box-shadow:0 1px 5px #ccd4e0;margin-bottom:16px}.n{font-size:28px;font-weight:700}.filters{display:flex;gap:8px;flex-wrap:wrap}select,input,button{padding:8px;border:1px solid #bbc5d3;border-radius:6px}button{background:#2864dc;color:white;cursor:pointer}table{width:100%;border-collapse:collapse}th,td{padding:9px;text-align:left;border-bottom:1px solid #e5eaf1}.bar{height:12px;background:#2864dc;border-radius:8px}.bad{color:#b3261e}.ok{color:#167544}.muted{color:#657085}dialog{width:min(900px,90%);border:0;border-radius:10px}a{color:#2864dc}</style></head>
<body><header><h1>Enterprise ETL Quality Dashboard</h1><div>Pipeline operations, validation and reconciliation</div></header><main>
<div class="filters"><select id="status"><option value="">All statuses</option><option>SUCCEEDED</option><option>QUARANTINED</option><option>FAILED</option><option>RUNNING</option></select><input id="page" type="number" min="1" value="1"><button onclick="loadAll()">Apply filters</button></div><br>
<div class="grid" id="cards"></div><section><h2>Pipeline runs</h2><table><thead><tr><th>Status</th><th>Started</th><th>Total</th><th>Accepted</th><th>Quarantined</th><th>Duplicates</th><th></th></tr></thead><tbody id="runs"></tbody></table><p id="pages" class="muted"></p></section>
<div class="grid"><section><h2>Validation failures</h2><div id="failures"></div></section><section><h2>Reconciliation differences</h2><div id="recons"></div></section></div>
<dialog id="detail"><button onclick="detail.close()">Close</button><h2>Run details</h2><div id="detailBody"></div></dialog></main>
<script>const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function api(path){let r=await fetch(path);if(!r.ok)throw Error(`Request failed (${r.status})`);return r.json()}
function bars(items,key){let m=Math.max(1,...items.map(x=>+x[key]));return items.map(x=>`<p>${esc(x.rule_id||x.metric_name)} <b>${esc(x[key])}</b><span class=bar style="display:block;width:${(+x[key]/m)*100}%"></span></p>`).join('')||'<p class=muted>No results</p>'}
async function loadAll(){try{let s=status.value,p=page.value;let [r,f,c]=await Promise.all([api(`/api/v1/runs?status=${s}&page=${p}&page_size=10`),api('/api/v1/validation-failures/summary'),api('/api/v1/reconciliations?status=FAIL&page_size=10')]);let totals=r.items.reduce((a,x)=>(a.total+=x.total_rows,a.accepted+=x.accepted_rows,a.quarantine+=x.quarantined_rows,a.duplicates+=x.duplicate_rows,a),{total:0,accepted:0,quarantine:0,duplicates:0});cards.innerHTML=Object.entries(totals).map(([k,v])=>`<div class=card><div class=n>${v}</div><div>${esc(k)}</div></div>`).join('');runs.innerHTML=r.items.map(x=>`<tr><td class=${x.status==='SUCCEEDED'?'ok':'bad'}>${esc(x.status)}</td><td>${esc(x.started_at)}</td><td>${x.total_rows}</td><td>${x.accepted_rows}</td><td>${x.quarantined_rows}</td><td>${x.duplicate_rows}</td><td><button onclick="drill('${x.run_id}')">View</button></td></tr>`).join('');pages.textContent=`Page ${r.page}; ${r.total} total runs`;failures.innerHTML=bars(f,'failure_count');recons.innerHTML=bars(c.items,'difference')}catch(e){cards.innerHTML=`<div class='card bad'>${esc(e.message)}</div>`}}
async function drill(id){let [run,batches,finance]=await Promise.all([api(`/api/v1/runs/${id}`),api(`/api/v1/runs/${id}/source-batches`),api(`/api/v1/runs/${id}/financial-totals`)]);detailBody.innerHTML=`<pre>${esc(JSON.stringify({run,batches:batches.items,finance},null,2))}</pre>`;detail.showModal()}loadAll();</script></body></html>"""

@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard() -> str:
    return HTML
