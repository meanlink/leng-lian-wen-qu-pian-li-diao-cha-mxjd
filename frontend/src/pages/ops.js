import {backupApi} from '../api/ops.js';
import {session} from '../stores/session.js';
import {workspace} from '../stores/workspace.js';
import {heading} from '../components/common/shell.js';
import {esc,date,notify} from '../utils/dom.js';
import {submit} from '../hooks/forms.js';

const stages = [['verify','核对'],['snapshot','回退快照'],['stage','暂存'],['switch','切换'],['files','替换归档'],['finalize','收尾']];
const restoreStatus = {pending:'等待中',running:'执行中',completed:'已完成',failed:'失败',interrupted:'已中断',cancelled:'已取消',rolled_back:'已回退'};
const stageIndex = stage => Math.max(0, stages.findIndex(s=>s[0]===stage));

function bytes(n){if(!n)return '0 B';const units=['B','KB','MB','GB'];let i=0,v=Number(n);while(v>=1024&&i<units.length-1){v/=1024;i++;}return v.toFixed(i?1:0)+' '+units[i];}

function verificationPanel(v){
 if(!v||!Object.keys(v).length)return '<p class="hint">尚未核对。恢复开始时会自动核对，也可先手动核对。</p>';
 const tables=Object.entries(v.tables||{}).map(([name,t])=>'<li><strong>'+esc(name)+'</strong> '+t.count+' 行 '+(t.ok?'✓':'✗')+'</li>').join('');
 const a=v.artifacts||{};
 return '<div class="verify-report"><p class="'+(v.ok?'ok':'bad')+'">'+(v.ok?'核对通过：结构与关联一致，文件指纹全部匹配':'核对未通过：' + esc(v.message||'备份包不可用'))+'</p><ul class="verify-list">'+tables+'</ul><p class="hint">归档文件 '+(a.verified||0)+'/'+(a.count||0)+' 通过'+(a.missing?.length?' · 缺失 '+a.missing.length:'')+(a.mismatched?.length?' · 指纹不符 '+a.mismatched.length:'')+' · 关联检查 '+(v.links?.checked||0)+' 条'+(v.warnings?.length?'<br>提示：'+v.warnings.map(esc).join('；'):'')+'</p></div>';
}

function progress(r){
 const idx=stageIndex(r.stage),pct=Math.max(0,Math.min(100,r.progress||0));
 return '<div class="restore-line"><div class="progress"><span style="width:'+pct+'%"></span></div><small>'+pct+'% · '+esc(r.message||'')+'</small></div><ol class="steps">'+stages.map((s,i)=>'<li class="'+(i<idx?'done':i===idx?(r.status==='running'?'live':'') :'')+'">'+esc(s[1])+'</li>').join('')+'</ol>';
}

function restoreItem(r,backup,admin){
 const active=r.status==='running'||r.status==='pending';
 const canResume=(r.status==='interrupted'||r.status==='failed')&&admin;
 const canCancel=(r.status==='running'||r.status==='pending'||r.status==='interrupted'||r.status==='failed')&&admin&&!(r.result?.db_switched)&&stageIndex(r.stage)<3;
 const canRollback=r.rollback_available&&admin&&r.mode!=='rollback'&&['completed','failed','interrupted'].includes(r.status);
 return '<article class="restore-item status-'+esc(r.status)+'"><div class="restore-head"><div><strong>恢复 #'+r.id+' · '+esc(backup?.name||('备份 #'+r.backup_id))+'</strong><p>'+esc(restoreStatus[r.status]||r.status)+' · '+(r.mode==='rollback'?'回退':'恢复')+' · 第 '+(r.attempt||1)+' 次尝试 · '+esc(r.created_by)+' · '+date(r.created_at)+'</p></div><span class="status '+esc(r.status)+'">'+esc(restoreStatus[r.status]||r.status)+'</span></div>'+progress(r)+(r.error?'<p class="warning">'+esc(r.error)+'</p>':'')+'<details '+(active||r.status==='failed'||r.status==='interrupted'?'open':'')+'><summary>核对与结果</summary>'+verificationPanel(r.verification)+(r.result?.counts?'<p class="hint">已恢复：'+Object.entries(r.result.counts).map(([k,v])=>esc(k)+' '+v).join(' · ')+' · 归档文件 '+r.result.artifacts+' 个 · 数据代际 '+r.result.generation+'</p>':'')+'</details><div class="restore-actions">'+(canResume?'<button class="small" data-resume="'+r.id+'">从检查点继续</button>':'')+(canCancel?'<button class="small" data-cancel="'+r.id+'">取消并保留原库</button>':'')+(canRollback?'<button class="small" data-rollback="'+r.id+'">回退到恢复前</button>':'')+'</div></article>';
}

export async function opsPage(){
 const [backups,restores,status]=await Promise.all([backupApi.list(),backupApi.restores(),backupApi.status()]);
 workspace.backups=backups;workspace.restores=restores;workspace.opsStatus=status;
 const admin=session.user.role==='admin';
 const completed=backups.filter(b=>b.status==='completed');
 const latest=restores[0];
 const running=restores.find(r=>r.status==='running'||r.status==='pending');
 const banner=status.maintenance?'<div class="maintenance-banner"><strong>恢复进行中</strong><span>'+esc(status.maintenance_reason||'业务写入与分发已暂停')+'</span>'+(running?'<a href="#/ops">查看恢复进度 #'+running.id+' →</a>':'')+'</div>':'';
 return heading('06 / RECOVERY','整库备份与恢复','备份包含数据批次、温区调查、分发计划与归档文件。恢复先核对关联和文件，坏包会被拒绝且原库保持不变。','<button class="button" id="refresh-ops">刷新状态 ↻</button>')+banner+'<section class="panel ops-status"><div class="section-title"><h2>运行状态</h2><span>恢复期间业务写入与分发暂停</span></div><div class="numbers compact-numbers"><div><span>数据代际</span><strong>'+status.generation+'</strong></div><div><span>备份</span><strong>'+backups.length+'</strong></div><div><span>可恢复点</span><strong>'+completed.length+'</strong></div><div><span>最近恢复</span><strong>'+(latest?esc(restoreStatus[latest.status]||latest.status):'—')+'</strong></div></div></section><div class="ops-grid"><section class="panel"><div class="section-title"><h2>创建整库备份</h2><span>含归档文件</span></div><form id="backup-form"><label>备份名称<input name="name" placeholder="例如：调参前基线" maxlength="160"></label><label>备注<textarea name="note" rows="2" maxlength="500" placeholder="记录这次备份的用途"></textarea></label><button type="submit" class="primary" '+(admin?'':'disabled')+'>开始整库备份</button><p class="hint">快照与运行中的计算、分发互不串版本：只捕获已提交的数据与对应归档。'+(admin?'':'仅负责人可创建备份。')+'</p></form></section><section class="panel"><div class="section-title"><h2>选择恢复点</h2><span>'+completed.length+' 个可用</span></div>'+(completed.length?'<div class="backup-list">'+completed.map(b=>'<article class="backup-item"><label class="radio-line"><input type="radio" name="restore-point" value="'+b.id+'" '+(b.id===completed[0].id?'checked':'')+'><span><strong>'+esc(b.name)+'</strong><small>'+date(b.created_at)+' · '+b.row_total+' 行 · 分发归档 '+b.artifact_count+' 组 · '+bytes(b.size_bytes)+' · 数据代际 '+b.generation+'</small>'+(b.note?'<small>'+esc(b.note)+'</small>':'')+'</span></label><div class="backup-actions"><button class="small" data-verify="'+b.id+'">核对备份</button><button class="small primary" data-restore="'+b.id+'" '+(admin&&!status.maintenance&&!status.backup_active?'':'disabled')+'>恢复到此处</button></div><div class="backup-verify" data-verify-report="'+b.id+'"></div></article>').join('')+'</div><p class="hint">恢复会整表替换业务数据：备份之后新增的数据不再可见，分发计划默认停用，排队中的调查会在恢复后继续执行。</p>':'<p class="empty">还没有完成的备份。先创建一次整库备份，再选择恢复点。</p>')+'</section></div><section class="panel"><div class="section-title"><h2>恢复进度</h2><span>可从中断处继续</span></div>'+(restores.length?'<div class="restore-list">'+restores.map(r=>restoreItem(r,backups.find(b=>b.id===r.backup_id),admin)).join('')+'</div>':'<p class="empty">暂无恢复记录。</p>')+'</section>';
}

export function bindOps(refresh){
 document.querySelector('#refresh-ops').onclick=refresh;
 submit(document.querySelector('#backup-form'),async f=>{const b=await backupApi.create({name:f.get('name'),note:f.get('note')});notify('备份已开始，完成后可选择为恢复点');poll(refresh,b.id);});
 document.querySelector('#backup-form').insertAdjacentHTML('afterend','<p class="hint" id="ops-poll">等待备份完成…</p>');
 document.querySelectorAll('[data-verify]').forEach(b=>b.onclick=async()=>{const box=document.querySelector('[data-verify-report="'+b.dataset.verify+'"]');b.disabled=true;try{const v=await backupApi.verify(b.dataset.verify);box.innerHTML=verificationPanel(v);notify('核对通过，可作为恢复点');}catch(e){box.innerHTML='<p class="warning">'+esc(e.message)+'</p>';notify(e.message,true);}finally{b.disabled=false;}});
 document.querySelectorAll('[data-restore]').forEach(b=>b.onclick=async()=>{const id=b.dataset.restore;const reason=prompt('恢复会替换当前全部业务数据，备份之后的新增数据将不可见。请输入恢复备注以确认：','误操作回滚');if(reason===null)return;b.disabled=true;try{const r=await backupApi.restore({backup_id:Number(id),note:reason});notify('恢复已开始，业务写入与分发暂时停止');pollRestore(refresh,r.id);}catch(e){notify(e.message,true);b.disabled=false;}});
 document.querySelectorAll('[data-resume]').forEach(b=>b.onclick=async()=>{try{const r=await backupApi.resume(b.dataset.resume);notify('已从检查点继续');pollRestore(refresh,r.id);}catch(e){notify(e.message,true);}});
 document.querySelectorAll('[data-cancel]').forEach(b=>b.onclick=async()=>{if(!confirm('取消本次恢复并保留原库？'))return;try{await backupApi.cancel(b.dataset.cancel);notify('已取消，原库未改动');await refresh();}catch(e){notify(e.message,true);}});
 document.querySelectorAll('[data-rollback]').forEach(b=>b.onclick=async()=>{if(!confirm('回退到恢复前快照？当前恢复结果将被覆盖。'))return;try{const r=await backupApi.rollback(b.dataset.rollback);notify('回退已开始');pollRestore(refresh,r.id);}catch(e){notify(e.message,true);}});
}

function poll(refresh,backupId){
 const tick=async()=>{try{const b=await backupApi.get(backupId);const box=document.querySelector('#ops-poll');if(box)box.textContent=b.status==='completed'?'备份完成：'+b.row_total+' 行、分发归档 '+b.artifact_count+' 组，可用于恢复。':b.status==='failed'?'备份失败：'+b.error:'备份中…';if(b.status==='creating'){setTimeout(tick,900);}else{await refresh();}}catch(e){notify(e.message,true);}};setTimeout(tick,700);
}
function pollRestore(refresh,id){
 const tick=async()=>{try{const r=await backupApi.restoreStatus(id);if(r.status==='running'||r.status==='pending'){await refresh();setTimeout(tick,900);}else{await refresh();notify(r.status==='completed'?'恢复完成：已回到所选备份时点':r.status==='failed'?'恢复失败：'+r.error:'恢复已中断，可从检查点继续',r.status!=='completed');}}catch(e){notify(e.message,true);}};setTimeout(tick,700);
}
