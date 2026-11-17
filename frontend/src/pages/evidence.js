import {evidence} from '../api/evidence.js';
import {workspace} from '../stores/workspace.js';
import {session} from '../stores/session.js';
import {heading} from '../components/common/shell.js';
import {esc,date,notify} from '../utils/dom.js';

const ACTION={approval:'批准',distribution:'分发',lost_receipt:'丢回执演练'};
const STATUS={pending:'待确认',confirmed:'已存证',blocked:'冻结待确认',conflict:'同号异内容'};
const BIZ={job:'分析结果',delivery:'简报分发',drill:'演练'};

function statusTag(value){return '<span class="ev-status '+esc(value)+'">'+esc(STATUS[value]||value)+'</span>';}

function shortHash(hash,size=18){return hash?esc(hash.slice(0,size))+'…':'—';}

function businessCell(event){
 const b=event.business||{};
 if(b.type==='job')return '<a href="'+esc(b.link||'#/results/'+b.id)+'">分析 #'+b.id+' · '+esc(b.name||'')+'</a><small>'+ (b.approved?'已批准':'未生效') +'</small>';
 if(b.type==='delivery')return '<a href="'+esc(b.link||'#/reports')+'">简报 #'+esc(b.id)+' · '+esc(b.name||'')+'</a><small>'+esc(b.status||'')+'</small>';
 return esc(b.type||event.biz_type)+' #'+esc(b.id||event.biz_id);
}

function eventRow(e){
 const station=(e.station_seq?'#'+e.station_seq:'—');
 return '<tr><td>'+statusTag(e.status)+'</td><td>'+esc(ACTION[e.action]||e.action)+'</td><td>'+esc(BIZ[e.biz_type]||e.biz_type)+'</td><td><code>'+esc(e.event_id)+'</code><small>对象 '+esc(e.object_ref)+'</small></td><td>'+esc(e.actor)+'</td><td>'+businessCell(e)+'</td><td><code>'+shortHash(e.content_sha256)+'</code><small>存证 '+esc(station)+' · 尝试 '+esc(e.attempts)+' 次</small></td><td>'+(e.status==='confirmed'?esc(date(e.confirmed_at)):e.last_error?esc(e.last_error):'等待存证站')+'</td></tr>';
}

function stationCard(station){
 const frozen=station.frozen;
 const anomalies=(station.last_verify&&station.last_verify.anomalies)||[];
 return '<section class="panel evidence-station '+(frozen?'frozen':'')+'"><div class="section-title"><h2>独立存证站</h2><span>'+(station.reachable===false?'不可达':'已连接 · '+esc(station.entries)+' 条 · 头 #'+esc(station.head_seq))+'</span></div><div class="evidence-meta">'+[['站点',station.reachable===false?'不可达':(frozen?'已冻结':'正常')],['链头哈希',shortHash(station.head_hash,26)],['最近核验',station.last_verify_at?date(station.last_verify_at):'尚未核验'],['隔离冲突',station.conflicts??'—']].map(([k,v])=>'<div><span>'+k+'</span><strong>'+esc(v)+'</strong></div>').join('')+'</div>'+(frozen?'<p class="warning">'+esc(station.freeze_reason||'存证链核验异常')+'</p>':'')+'</section>';
}

function anomaliesCard(data){
 const anomalies=data.anomalies||[];
 return '<section class="panel"><div class="section-title"><h2>异常与隔离</h2><span>'+(anomalies.length+' 项核验异常 · '+data.conflicts.length+' 项同号异内容')+'</span></div>'+(anomalies.length?'<div class="table-wrap"><table><thead><tr><th>类型</th><th>序号</th><th>说明</th></tr></thead><tbody>'+anomalies.map(a=>'<tr><td>'+esc(a.type)+'</td><td>'+esc(a.seq)+'</td><td>'+esc(a.detail)+'</td></tr>').join('')+'</tbody></table></div>':'<p class="empty">最近一次核验未发现删改或中间缺项。</p>')+(data.conflicts.length?'<div class="section-title"><h3>同号异内容隔离记录</h3></div>'+data.conflicts.map(c=>'<div class="evidence-conflict"><strong>'+esc(BIZ[c.biz_type]||c.biz_type)+' #'+esc(c.biz_id)+' · '+esc(ACTION[c.action]||c.action)+'</strong><p>'+esc(c.detail)+'</p><small>存证编号 '+esc(c.event_id)+' · 已存证指纹 '+shortHash(c.existing_content_sha256)+' · 新内容指纹 '+shortHash(c.incoming_content_sha256)+'</small></div>').join(''):'')+'</section>';
}

export async function evidencePage(){
 const data=await evidence.overview();workspace.evidence=data;
 const pending=data.events.filter(e=>e.status==='pending'||e.status==='blocked');
 const isAdmin=session.user.role==='admin';
 const drillButtons=isAdmin&&data.drills_enabled?'<button class="button" id="drill-lost">演练：丢回执后重试 ↻</button><button class="button" id="drill-tamper">演练：模拟删改并核验 ⚠</button>':'';
 return heading('06 / EVIDENCE','独立存证与核验','批准与简报分发在独立存证站按发生顺序留痕：操作者、对象、内容指纹逐条哈希串联。确认前业务结果不算获批、简报不可下载。',(isAdmin?'<div class="heading-actions"><button class="button" id="ev-verify">立即核验</button><button class="button" id="ev-repair">补齐并复验</button><button class="button" id="ev-retry">重试待确认</button></div>':''))+
 stationCard(data.station)+
 '<div class="evidence-counts">'+[['待确认',(()=>{const p=data.events.filter(e=>e.status==='pending').length;return p})(),'pending'],['已存证',data.events.filter(e=>e.status==='confirmed').length,'confirmed'],['冻结/阻断',data.events.filter(e=>e.status==='blocked').length,'blocked'],['同号异内容',data.events.filter(e=>e.status==='conflict').length,'conflict']].map(([k,v,s])=>'<div class="ev-count '+s+'"><span>'+k+'</span><strong>'+v+'</strong></div>').join('')+'</div>'+
 '<section class="panel"><div class="section-title"><h2>待确认</h2><span>业务已登记、存证未确认的动作不计为生效</span></div>'+(pending.length?'<div class="table-wrap"><table><thead><tr><th>状态</th><th>动作</th><th>业务类型</th><th>存证编号 / 对象</th><th>操作者</th><th>业务对应</th><th>内容指纹</th><th>说明</th></tr></thead><tbody>'+pending.map(eventRow).join('')+'</tbody></table></div>':'<p class="empty">没有待确认的存证。批准与分发确认后才会出现在这里。</p>')+'</section>'+
 '<section class="panel"><div class="section-title"><h2>全部存证链</h2><span>按存证站确认顺序排列 · 最近 200 条</span></div><div class="table-wrap"><table><thead><tr><th>状态</th><th>动作</th><th>业务类型</th><th>存证编号 / 对象</th><th>操作者</th><th>业务对应</th><th>内容指纹</th><th>确认/说明</th></tr></thead><tbody>'+data.events.map(eventRow).join('')+'</tbody></table></div></section>'+
 anomaliesCard(data)+
 (isAdmin?'<section class="panel drills"><div class="section-title"><h2>演练</h2><span>本地模拟，不改写已存证内容</span></div><p class="hint">丢回执演练会写入一条演练存证后丢弃响应，再按同一存证编号重试，页面显示存证站返回“重放”而非新增。篡改演练在存储层删除或改写中间条目，核验后立即冻结后续批准与分发；点击“补齐并复验”从只追加镜像恢复，恢复后解除冻结。</p><div class="drill-actions">'+drillButtons+'</div><div id="drill-result" class="drill-result"></div></section>':'');
}

function showDrill(text,isError){
 const box=document.querySelector('#drill-result');if(!box)return;box.classList.toggle('error',!!isError);box.textContent=text;
}

export function bindEvidence(refresh){
 const admin=session.user.role==='admin';
 const guard=fn=>async()=>{try{await fn();}catch(e){notify(e.message,true);}};
 document.querySelector('#ev-verify')?.addEventListener('click',guard(async()=>{const r=await evidence.verify();notify(r.ok?'核验通过：存证链完整，未发现删改或中间缺项':'核验发现异常，已冻结后续批准与分发',!r.ok);await refresh();}));
 document.querySelector('#ev-repair')?.addEventListener('click',guard(async()=>{const r=await evidence.repair();notify('已补齐 '+(r.restored||0)+' 条、隔离 '+(r.quarantined||0)+' 条；复验'+(r.verify?.ok?'通过':'仍未通过'),!r.verify?.ok);await refresh();}));
 document.querySelector('#ev-retry')?.addEventListener('click',guard(async()=>{const r=await evidence.retry();notify('重试完成：确认 '+r.confirmed+' 条，待确认 '+r.still_pending+' 条'+(r.frozen?'，存证链已冻结':'') ,r.frozen);await refresh();}));
 document.querySelector('#drill-lost')?.addEventListener('click',guard(async()=>{showDrill('正在执行丢回执演练…');const r=await evidence.drillLostReceipt();showDrill('存证 '+r.event_id+'：写入后回执丢失（HTTP '+r.dropped_receipt_status+'），按同一编号重试 → 存证站返回'+(r.replayed?'重放':'新增')+'；存证总数 '+r.entries_before+' → '+r.entries_after+'。'+(r.message||''));await refresh();}));
 document.querySelector('#drill-tamper')?.addEventListener('click',guard(async()=>{showDrill('正在模拟存储层删改并核验…');const r=await evidence.drillTamper('delete_middle');const v=r.verify||{};showDrill('已删除序号 #'+(r.tamper?.target_seq)+' 的存储行；核验 '+(v.ok?'未见异常':'发现 '+((v.anomalies||[]).length)+' 项异常')+'，存证链'+(v.frozen?'已冻结，批准与分发将返回 423':'未冻结')+'。点击“补齐并复验”恢复。',!v.ok);await refresh();}));
 if(!admin)return;
}
