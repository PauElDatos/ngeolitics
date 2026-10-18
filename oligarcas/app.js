'use strict';

const $ = s => document.querySelector(s);
const colors = ['#67b6df', '#69c7ae', '#bdcd73', '#eeb55e', '#fef702'];
const flags = { ES:'🇪🇸', US:'🇺🇸', DE:'🇩🇪', FR:'🇫🇷', IT:'🇮🇹', NL:'🇳🇱', JP:'🇯🇵', GB:'🇬🇧', CA:'🇨🇦', KR:'🇰🇷', MX:'🇲🇽' };
const state = { country:'ES', view:'quintiles', metric:'share', scale:'linear', limit:100, ranking:'top20', compareStyle:'rank', compositionGroup:'p0p50', compositionSource:'recent' };
let data;
let exportRows = [];
const nfmt = (v, digits=1) => v == null || !Number.isFinite(v) ? 'No disponible' : new Intl.NumberFormat('es-ES', {maximumFractionDigits:digits, minimumFractionDigits:digits}).format(Object.is(v,-0)?0:v);
const pct = v => v == null ? 'No disponible' : `${nfmt(v * 100, 2)} %`;
const money = v => v == null ? 'No disponible' : `${nfmt(v,0)} €`;
function compact(v) {
  if (v == null) return 'No disponible';
  const a = Math.abs(v);
  if (a >= 1e12) return `${nfmt(v/1e12,2)} bill. €`;
  if (a >= 1e9) return `${nfmt(v/1e9,2)} mil M€`;
  if (a >= 1e6) return `${nfmt(v/1e6,2)} M€`;
  if (a >= 1e3) return `${nfmt(v/1e3,1)} mil €`;
  return money(v);
}
const esc = x => String(x ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const safeURL = u => /^https?:\/\//i.test(u || '') ? esc(u) : '#';
const countryName = c => data.countries.find(x => x.code === c)?.name ?? c;
const nat = (c=state.country, period=2024) => data.national.find(x => x.country === c && x.period === period);
const groups = (category, c=state.country) => data.groups.filter(g => g.country === c && g.category === category);
const group = (id, c=state.country) => data.groups.find(g => g.country === c && g.group === id && g.category !== 'percentile') ?? data.groups.find(g => g.country === c && g.group === id);
const src = id => data.sources.find(s => s.id === id || s.source_id === id);
const sourceLink = id => { const s=src(id); return s ? `<a href="${safeURL(s.url)}" target="_blank" rel="noopener">${esc(s.title || s.source_id)} ↗</a>` : esc(id); };
const pLabel = p => nfmt(p, Number.isInteger(p)?0: Math.min(3,String(p).split('.')[1]?.length ?? 0));
function groupName(id) {
  const names = {p0p100:'Toda la población', p0p50:'50 % inferior', p0p90:'90 % inferior', p90p100:'10 % superior', p95p100:'5 % superior', p99p100:'1 % superior', 'p99.9p100':'0,1 % superior', 'p99.99p100':'0,01 % superior', 'p99.999p100':'0,001 % superior', 'p99p99.9':'P99–P99,9', 'p99.9p99.99':'P99,9–P99,99', 'p99.99p99.999':'P99,99–P99,999'};
  if (names[id]) return names[id];
  const m = id.match(/^p([\d.]+)p([\d.]+)$/);
  return m ? `P${pLabel(+m[1])}–P${pLabel(+m[2])}` : id;
}
function activate(view, push=true) {
  if (!['quintiles','personas','cima','comparar','espana-usa','nivel','composicion','fuentes'].includes(view)) view='quintiles';
  state.view=view;
  if (push) history.replaceState(null,'',`#${view}?pais=${state.country}`);
  render();
}
function segment(values, selected, action) {
  return `<div class="segment">${values.map(([value,label]) => `<button data-${action}="${value}" class="${selected===value?'on':''}" aria-pressed="${selected===value}">${label}</button>`).join('')}</div>`;
}
function setHead(kicker,title,sub,controls='') {
  $('#viewKicker').textContent=kicker; $('#viewTitle').textContent=title; $('#viewSubtitle').textContent=sub; $('#controls').innerHTML=controls;
}
function setFoot(text, c=state.country) {
  $('#chartFoot').innerHTML=`${esc(countryName(c))} · 2024 · Adultos de 20 años o más, patrimonio repartido por igual dentro de las parejas (equal-split). Patrimonio neto: activos menos deudas. ${text} Fuente: ${sourceLink('wid:WID_'+c+'_data')}. Estimaciones publicadas/modelizadas, grado 0.`;
}
function setInternationalFoot(text) {
  $('#chartFoot').innerHTML=`Once países · 2024 · Adultos de 20 años o más, equal-split · Patrimonio neto, activos menos deudas. ${text} Fuente: World Inequality Database, <a href="#fuentes" data-view="fuentes">series originales de los once países ↗</a>. Estimaciones publicadas/modelizadas, grado 0.`;
}
function insight(title,text) { return `<article class="insight"><h3>${title}</h3><p>${text}</p></article>`; }
function table(headers,rows,title='Datos por grupo') {
  $('#tableTitle').textContent=title;
  $('#table').innerHTML=`<table><thead><tr>${headers.map(x=>`<th scope="col">${x}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr${row.selected?' class="selected"':''}>${row.cells.map((x,i)=>i===0?`<td>${x}</td>`:`<td>${x}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
  $('#detailPanel').hidden=false;
}
function groupTable(gs,title='Patrimonio de cada grupo') {
  table(['Grupo','Cuota','Patrimonio total','Media por adulto','Mediana del grupo','Desde','Hasta','Adultos estimados'],gs.map((g,i)=>({cells:[g.category==='quintile'?`Q${i+1} · ${groupName(g.group)}`:groupName(g.group),pct(g.share),compact(g.total),money(g.mean),money(g.median),money(g.lower),g.b===100?'Sin límite superior':money(g.upper),nfmt(g.count,0)]})),title);
  exportRows=[['pais','periodo','unidad','categoria','grupo','cuota_fraccion','total_EUR_2025','media_EUR_2025','mediana_EUR_2025','umbral_inferior_EUR_2025','umbral_superior_EUR_2025','adultos_estimados','fuente','ids_cuota'],...gs.map(g=>[g.country,g.period,'adultos_equal_split',g.category,g.group,g.share,g.total,g.mean,g.median,g.lower,g.upper,g.count,g.source,g.provenance.share.input_ids.join('|')])];
  addTrace(gs);
}
function addTrace(gs) {
  $('#detailPanel .tracePanel')?.remove();
  $('#detailPanel').insertAdjacentHTML('beforeend',`<details class="tracePanel"><summary>Registros originales y fórmulas de esta tabla</summary><p>Las cuotas son fracciones de la riqueza nacional. Los importes se presentan en EUR de 2025. Las medias y umbrales se convierten por el tipo de mercado del BCE incluido en el JSON. La mediana del grupo es el umbral del percentil central publicado, cuando existe; no una interpolación.</p><pre>${esc(JSON.stringify(gs.map(g=>({country:g.country,category:g.category,group:g.group,source:g.source,provenance:g.provenance})),null,2))}</pre></details>`);
}
function renderStats() {
  const n=nat(), q=group('p80p100'), one=group('p99p100');
  $('#stats').innerHTML=[
    ['Patrimonio medio',compact(n.mean),'Por adulto · EUR de 2025'],
    ['Patrimonio mediano',compact(n.median),'La mitad queda por debajo'],
    ['Riqueza del 20 % superior',pct(q.share),'Quintil Q5 · cuota nacional'],
    ['Riqueza del 1 % superior',pct(one.share),`${countryName(state.country)} · WID 2024`],
  ].map(([k,v,s])=>`<article class="statCard"><div class="k">${k}</div><div class="v">${v}</div><div class="s">${esc(s)}</div></article>`).join('');
}
function barRows(gs,metric='share',small=false) {
  const all = data.groups.filter(g=>g.category==='quintile');
  const min=metric==='share'?Math.min(0,...all.map(g=>g.share*100)):Math.min(0,...all.map(g=>g.mean));
  const max=metric==='share'?100:Math.max(...all.map(g=>g.mean))*1.02;
  const zero=-min/(max-min)*100;
  const html=gs.map((g,i)=>{
    const v=metric==='share'?g.share*100:g.mean, width=Math.abs(v)/(max-min)*100;
    const left=v<0?zero-width:zero;
    return `<div class="quintileRow"><div class="groupLabel">Q${i+1}${small?'':`<small>${i===0?'20 % con menos':i===4?'20 % con más':`Percentiles ${g.a}–${g.b}`}${i===0||i===4?' patrimonio':''}</small>`}</div><div class="barTrack" data-tip="${esc(`Q${i+1} · ${groupName(g.group)}|Cuota: ${pct(g.share)}|Media: ${money(g.mean)}|Adultos estimados: ${nfmt(g.count,0)}`)}" tabindex="0"><span class="zero" style="left:${zero}%"></span><span class="bar" style="left:${left}%;width:${width}%;background:${colors[i]}"></span></div><span class="value ${v<0?'negative':''}">${metric==='share'?pct(g.share):compact(g.mean)}</span></div>`;
  }).join('');
  return {html,min,max,zero};
}
function renderQuintiles() {
  const name=countryName(state.country),gs=groups('quintile'), n=nat();
  setHead('01 / El reparto',`${name}: cinco grupos, la misma población`,'Cada quintil representa al 20 % de los adultos, ordenados de menor a mayor patrimonio.',segment([['share','Proporción de riqueza'],['mean','Patrimonio medio']],state.metric,'metric'));
  const bars=barRows(gs,state.metric);
  $('#visual').innerHTML=`<div class="distributionHeader"><strong>${state.metric==='share'?'¿Qué parte tiene cada quintil?':'¿Cuánto tiene de media cada adulto?'}</strong><span>${state.metric==='share'?'% del patrimonio nacional':'EUR de 2025 · escala común'}</span></div><div class="quintileChart">${bars.html}</div><div class="chartAxis">${[0,25,50,75,100].map(v=>`<span style="left:${bars.zero+(100-bars.zero)*v/100}%">${state.metric==='share'?v+' %':compact(bars.max*v/100)}</span>`).join('')}</div><div class="populationStrip">${gs.map((g,i)=>`<div class="populationQuintile"><div class="people" aria-hidden="true">${Array.from({length:20},()=>`<i class="person" style="background:${colors[i]}"></i>`).join('')}</div><span>Q${i+1} · 20 de cada 100</span></div>`).join('')}</div>`;
  setFoot(`Cuotas publicadas sin renormalizar. Escala lineal común a los once países; los valores negativos se extienden a la izquierda del cero. ${state.metric==='mean'?'Media por adulto en EUR de 2025.':''}`);
  const bottom=gs[0].share+gs[1].share, middle=gs[2].share+gs[3].share, top=gs[4].share, fifty=group('p0p50'),ninety=group('p0p90');
  const rank=data.countries.map(c=>({c:c.code,s:group('p80p100',c.code).share})).sort((a,b)=>b.s-a.s).findIndex(x=>x.c===state.country)+1;
  $('#insights').innerHTML=insight('La población pesa lo mismo; su patrimonio, no.',`El 40 % inferior (Q1 + Q2) reúne <strong>${pct(bottom)}</strong>; el 40 % intermedio (Q3 + Q4), <strong>${pct(middle)}</strong>. El 20 % superior concentra <strong>${pct(top)}</strong>.`)+insight(`${name} en perspectiva`,`El 50 % inferior reúne <strong>${pct(fifty.share)}</strong> y el 90 % inferior, <strong>${pct(ninety.share)}</strong>. ${name} ocupa el puesto <strong>${rank} de 11</strong> por cuota del quintil superior, de mayor a menor concentración. La media nacional es ${n.median>0?'<strong>'+nfmt(n.mean/n.median,2)+' veces</strong> la mediana.':'incomparable con una mediana no positiva.'}`);
  groupTable(gs,'Cuotas, importes y umbrales por quintil');
  $('#insights').insertAdjacentHTML('beforeend',insight('Qué significa un cero publicado','Una cuota de 0,00 % puede responder al redondeo de la fuente. No significa que todos los adultos del grupo tengan exactamente cero patrimonio. Una media negativa indica que las deudas del grupo superan sus activos en conjunto.'));
}
function renderPeople() {
  const gs=groups('percentile').filter(g=>g.b<=state.limit).sort((a,b)=>a.a-b.a);
  setHead('02 / Cien partes de la población',`${countryName(state.country)}, reducido a 100 adultos`,'Una barra representa la media de un intervalo del 1 % de adultos; no una persona real.',segment([['linear','Lineal'],['symlog','Log. con signo']],state.scale,'scale')+`<label class="field">Ampliar tramo<select id="percentileLimit"><option value="100" ${state.limit===100?'selected':''}>P0–P100 · todos</option><option value="99" ${state.limit===99?'selected':''}>P0–P99 · excluye el 1 % superior</option><option value="90" ${state.limit===90?'selected':''}>P0–P90 · excluye el 10 % superior</option></select></label>`);
  const W=1060,H=410,L=85,R=20,T=25,B=60,pw=W-L-R,ph=H-T-B;
  const trans=v=>state.scale==='symlog'?Math.sign(v)*Math.log10(1+Math.abs(v)/10000):v;
  const sameGroups=data.groups.filter(g=>g.category==='percentile'&&g.b<=state.limit);
  const vMin=Math.min(0,...sameGroups.map(g=>g.mean)),vMax=Math.max(...sameGroups.map(g=>g.mean))*1.04;
  const min=trans(vMin),max=trans(vMax),y=v=>T+ph-(trans(v)-min)/(max-min)*ph,zero=y(0),bw=pw/state.limit;
  let grid='';
  for(let i=0;i<=5;i++){
    const tv=min+(max-min)*i/5;
    const original=state.scale==='symlog'?Math.sign(tv)*(10**Math.abs(tv)-1)*10000:tv;
    const yy=y(original);
    grid+=`<line x1="${L}" x2="${W-R}" y1="${yy}" y2="${yy}" stroke="#434343" stroke-dasharray="3 5"/><text x="${L-12}" y="${yy+4}" fill="#aaa" text-anchor="end" font-size="11">${esc(compact(original))}</text>`;
  }
  const bars=gs.map(g=>{
    const yy=y(g.mean),h=Math.abs(yy-zero),x=L+g.a*bw;
    const tip=`${groupName(g.group)} · ${nfmt(g.count,0)} adultos|Media: ${money(g.mean)}|Cuota nacional: ${pct(g.share)}|Umbral de entrada: ${money(g.lower)}`;
    return `<rect x="${x+1}" y="${Math.min(yy,zero)}" width="${Math.max(1,bw-2)}" height="${Math.max(.7,h)}" fill="${colors[Math.min(4,Math.floor(g.a/20))]}" tabindex="0" role="img" aria-label="${esc(tip.replaceAll('|','. '))}" data-tip="${esc(tip)}"/>`;
  }).join('');
  let labels='';for(let i=0;i<=state.limit;i+=10) labels+=`<text x="${L+i*bw}" y="${H-30}" fill="#aaa" text-anchor="middle" font-size="11">${i} %</text>`;
  $('#visual').innerHTML=`<div class="legend">${colors.map((c,i)=>`<span><i class="dot" style="background:${c}"></i>Q${i+1}</span>`).join('')}<span>Patrimonio medio de cada intervalo · EUR de 2025</span></div><div class="hundredWrap"><svg class="svgChart percentileChart" viewBox="0 0 ${W} ${H}" role="img" aria-label="Patrimonio medio por percentil, con valores negativos bajo el cero"><title>Distribución en cien intervalos, ${esc(countryName(state.country))}</title>${grid}<line x1="${L}" x2="${W-R}" y1="${zero}" y2="${zero}" stroke="#aaa"/>${bars}${labels}<text x="${W/2}" y="${H-3}" text-anchor="middle" fill="#aaa" font-size="11">Adultos ordenados por patrimonio: de menos a más →</text></svg></div><p class="caption">${state.limit<100?`Ampliación: se muestra hasta P${state.limit}. El ${100-state.limit} % superior queda fuera de esta vista. `:''}${state.scale==='symlog'?'Escala logarítmica con signo: sign(x) × log₁₀(1 + |x| / 10.000 €). Comprime los extremos y conserva cero y negativos; las alturas no expresan proporciones lineales.':'Escala lineal: alturas proporcionales al patrimonio medio. La escala es común para los once países dentro del tramo mostrado.'}</p>`;
  setFoot('Cien intervalos publicados por WID, sin interpolación propia. Las estimaciones de grado 0 permiten esta representación del modelo publicado; no son cien observaciones directas.');
  const one=group('p99p100'),p=nat().population/100;
  $('#insights').innerHTML=insight('Una barra contiene a muchos adultos',`Cada intervalo del 1 % representa aproximadamente <strong>${nfmt(p,0)} adultos</strong>. La barra muestra su media, que puede ocultar diferencias muy grandes dentro del grupo.`)+insight('La última barra también oculta una distribución',`La media del 1 % superior es <strong>${money(one.mean)}</strong>, pero su umbral de entrada es <strong>${money(one.lower)}</strong>. Abre «Dentro del 1 %» para ampliar la parte superior hasta el 0,001 %.`);
  groupTable(gs,'Cien intervalos publicados de un punto percentil');
}
function renderTop() {
  const ids=['p99p100','p99.9p100','p99.99p100','p99.999p100'],gs=ids.map(id=>group(id)),base=gs[0];
  const exclusive=groups('top_nonoverlapping').sort((a,b)=>a.a-b.a);
  setHead('03 / Ampliar la cima','El 1 % contiene muchos mundos','Del 1 % al 0,001 %: grupos cada vez más pequeños dentro de la misma distribución.');
  $('#visual').innerHTML=`<div class="topCards">${gs.map(g=>`<article class="topCard"><div class="topName">${groupName(g.group)}</div><strong>${pct(g.share)}</strong><p>del patrimonio nacional</p><div class="miniBar"><i style="width:${g.share*100}%"></i></div><p style="margin-top:14px">Media por adulto<br><b>${compact(g.mean)}</b></p><p style="margin-top:8px">Entrada desde ${compact(g.lower)}<br>≈ ${nfmt(g.count,0)} adultos</p></article>`).join('')}</div><h3 class="subheading">Cómo se reparte la riqueza dentro del 1 %</h3><p class="caption">Intervalos sin solapamiento. La barra completa representa el patrimonio del 1 % superior.</p><div class="nonoverlap">${exclusive.map((g,i)=>`<div tabindex="0" style="width:${g.share/base.share*100}%;background:${colors[i+1]}" data-tip="${esc(`${groupName(g.group)}|${pct(g.share)} del patrimonio nacional|${pct(g.share/base.share)} de la riqueza del 1 %`)}">${nfmt(g.share/base.share*100,1)} %</div>`).join('')}</div><div class="legend">${exclusive.map((g,i)=>`<span><i class="dot" style="background:${colors[i+1]}"></i>${groupName(g.group)}</span>`).join('')}</div><p class="caption">Las cuatro cuotas superiores son acumuladas: el 0,001 % pertenece al 0,01 %, que pertenece al 0,1 %, que pertenece al 1 %. No se suman entre sí.</p>`;
  setFoot('La tabla distingue la media del grupo del umbral para entrar. Número de adultos = población adulta × fracción del grupo; es una estimación que puede ser fraccional.');
  const ratio=nat().median>0?gs[3].mean/nat().median:null;
  $('#insights').innerHTML=insight('La concentración continúa dentro de la cima',`El 0,1 % superior posee <strong>${pct(gs[1].share/base.share)}</strong> de la riqueza del 1 % superior. El 0,001 % representa una de cada 100.000 personas adultas y reúne <strong>${pct(gs[3].share)}</strong> del patrimonio nacional.`)+insight('Media y umbral responden a preguntas distintas',`La media del 0,001 % es <strong>${compact(gs[3].mean)}</strong>; se entra desde <strong>${compact(gs[3].lower)}</strong>. ${ratio!==null?'Su media equivale a <strong>'+nfmt(ratio,0)+' veces</strong> la mediana nacional.':'La mediana nacional no permite calcular un ratio interpretable.'} Las colas extremas dependen especialmente de la modelización.`);
  groupTable([group('p90p100'),group('p95p100'),...gs,...exclusive],'Grupos acumulados e intervalos exclusivos (no sumarlos)');
}
const rankings = {
  top20:{label:'Cuota del 20 % superior',value:c=>group('p80p100',c).share,format:pct,desc:true},
  bottom40:{label:'Cuota del 40 % inferior',value:c=>groups('quintile',c).slice(0,2).reduce((a,g)=>a+g.share,0),format:pct,desc:true},
  middle40:{label:'Cuota del 40 % intermedio',value:c=>groups('quintile',c).slice(2,4).reduce((a,g)=>a+g.share,0),format:pct,desc:true},
  bottom50:{label:'Cuota del 50 % inferior',value:c=>group('p0p50',c).share,format:pct,desc:true},
  top1:{label:'Cuota del 1 % superior',value:c=>group('p99p100',c).share,format:pct,desc:true},
  top01:{label:'Cuota del 0,1 % superior',value:c=>group('p99.9p100',c).share,format:pct,desc:true},
  gini:{label:'Gini patrimonial',value:c=>nat(c).gini,format:v=>nfmt(v,3),desc:true},
  median:{label:'Patrimonio mediano',value:c=>nat(c).median,format:compact,desc:true},
  ratio:{label:'Ratio media / mediana',value:c=>nat(c).median>0?nat(c).mean/nat(c).median:null,format:v=>nfmt(v,2)+' ×',desc:true},
};
function renderCompare() {
  const r=rankings[state.ranking],ns=data.countries.map(c=>({...c,v:r.value(c.code)})).sort((a,b)=>(b.v??-Infinity)-(a.v??-Infinity));
  setHead('04 / La misma medida', 'Once países, una comparación',`Orden de mayor a menor: ${r.label.toLowerCase()}. El país seleccionado aparece destacado.`,segment([['rank','Clasificación'],['quintiles','Todos los quintiles']],state.compareStyle,'compare-style')+`<label class="field">Ordenar por<select id="ranking">${Object.entries(rankings).map(([k,v])=>`<option value="${k}" ${k===state.ranking?'selected':''}>${v.label}</option>`).join('')}</select></label>`);
  const min=Math.min(0,...ns.map(c=>c.v??0)),max=Math.max(...ns.map(c=>c.v??0))*1.06,range=max-min,zero=-min/range*100;
  $('#visual').innerHTML=`<div class="distributionHeader"><strong>${r.label}</strong><span>${state.ranking==='gini'?'Índice de 0 a 1':state.ranking==='median'?'EUR de 2025':state.ranking==='ratio'?'Veces la mediana':'% del patrimonio nacional'}</span></div><div class="rankChart">${ns.map((c,i)=>`<div class="rankRow ${c.code===state.country?'selected':''}"><div class="rankCountry"><span class="rankNumber">${i+1}</span>${esc(c.name)}</div><div class="barTrack" data-tip="${esc(`${c.name}|${r.label}: ${r.format(c.v)}`)}" tabindex="0"><span class="zero" style="left:${zero}%"></span><span class="bar" style="background:${c.code===state.country?'#fef702':'#899a9e'};left:${c.v<0?zero-Math.abs(c.v)/range*100:zero}%;width:${Math.abs(c.v??0)/range*100}%"></span></div><span class="value ${c.v<0?'negative':''}">${r.format(c.v)}</span></div>`).join('')}</div>`;
  if(state.compareStyle==='quintiles') {
    const signed=ns.map(c=>{const gs=groups('quintile',c.code);return {...c,gs,pos:gs.reduce((s,g)=>s+Math.max(0,g.share),0),neg:gs.reduce((s,g)=>s+Math.min(0,g.share),0)};});
    const lo=Math.min(0,...signed.map(c=>c.neg)),hi=Math.max(...signed.map(c=>c.pos)),span=hi-lo,z=-lo/span*100;
    $('#visual').innerHTML=`<div class="legend">${colors.map((color,i)=>`<span><i class="dot" style="background:${color}"></i>Q${i+1}</span>`).join('')}<span>Barra = reparto nacional · valores negativos a la izquierda del cero</span></div><div class="rankChart">${signed.map((c,i)=>{let pos=0,neg=0;const pieces=c.gs.map((g,j)=>{const start=g.share<0?neg+g.share:pos;if(g.share<0)neg+=g.share;else pos+=g.share;return `<span class="bar" tabindex="0" data-tip="${esc(`${c.name} · Q${j+1}|${pct(g.share)} del patrimonio nacional`)}" style="border-radius:0;left:${(start-lo)/span*100}%;width:${Math.abs(g.share)/span*100}%;background:${colors[j]}"></span>`;}).join('');return `<div class="rankRow ${c.code===state.country?'selected':''}"><div class="rankCountry"><span class="rankNumber">${i+1}</span>${esc(c.name)}</div><div class="barTrack" style="height:29px">${pieces}<span class="zero" style="left:${z}%"></span></div><span style="font-size:11px;text-align:right;color:#bbb">Q5: ${pct(c.gs[4].share)}</span></div>`;}).join('')}</div><p class="caption">Escala lineal idéntica y colores constantes. Las cuotas negativas no se recortan ni se redistribuyen. Las cuotas positivas pueden sumar más del 100 % cuando hay patrimonio negativo en otros grupos.</p>`;
  }
  setInternationalFoot('Mismo año y definición WID. Escala lineal común. El orden depende del indicador elegido: una cuota inferior alta y una cuota superior alta tienen interpretaciones distintas.');
  const es=r.value('ES'),us=r.value('US'),position=ns.findIndex(c=>c.code==='ES')+1;
  $('#insights').innerHTML=insight('La posición de España',`España ocupa el puesto <strong>${position} de 11</strong> en ${r.label.toLowerCase()}, con <strong>${r.format(es)}</strong>. ${['median','ratio','gini'].includes(state.ranking)?'El indicador seleccionado mide '+(state.ranking==='median'?'nivel de patrimonio en el centro de la distribución.':state.ranking==='ratio'?'la distancia relativa entre media y mediana.':'desigualdad del conjunto de la distribución.'):`La diferencia España − EE. UU. es <strong>${nfmt((es-us)*100,2)} puntos porcentuales</strong>.`}`)+insight('Nivel y distribución son dos ejes distintos','Un país puede tener más patrimonio por adulto y repartirlo de forma más desigual. Compara las cuotas y el Gini para estudiar concentración; consulta «Media y mediana» para estudiar cantidades. Los importes en euros de mercado no miden poder adquisitivo.');
  table(['País','Q1','Q2','Q3','Q4','Q5','50 % inferior','1 % superior','Mediana (EUR)','Gini'],ns.map(c=>({selected:c.code===state.country,cells:[esc(c.name),...groups('quintile',c.code).map(g=>pct(g.share)),pct(group('p0p50',c.code).share),pct(group('p99p100',c.code).share),money(nat(c.code).median),nfmt(nat(c.code).gini,4)]})),'Comparación homogénea · WID 2024');
  exportRows=[['pais','periodo','Q1','Q2','Q3','Q4','Q5','bottom50','top1','mediana_EUR_2025','gini'],...ns.map(c=>[c.code,2024,...groups('quintile',c.code).map(g=>g.share),group('p0p50',c.code).share,group('p99p100',c.code).share,nat(c.code).median,nat(c.code).gini])];
}
function renderPair() {
  setHead('05 / La referencia española','España frente a Estados Unidos','Mismo año, unidad estadística, definición patrimonial y base de precios.');
  $('#visual').innerHTML=`<div class="comparePair">${['ES','US'].map(c=>`<div class="compareCountry"><h3>${flags[c]} ${countryName(c)}</h3>${barRows(groups('quintile',c),'share',true).html}<p class="caption">Patrimonio mediano: <strong>${money(nat(c).median)}</strong><br>Patrimonio medio: <strong>${money(nat(c).mean)}</strong></p></div>`).join('')}</div><p class="caption">Barras en la misma escala lineal: 0–100 % del patrimonio nacional, con espacio a la izquierda para cuotas negativas.</p>`;
  $('#chartFoot').innerHTML=`España y Estados Unidos · 2024 · Adultos de 20 años o más, equal-split · Patrimonio neto. Importes en EUR de 2025, conversión BCE a tipos de mercado, sin PPA. Fuentes: ${sourceLink('wid:WID_ES_data')} y ${sourceLink('wid:WID_US_data')}. Estimaciones de grado 0.`;
  const es=group('p80p100','ES'),us=group('p80p100','US'),ones=group('p99p100','ES'),onus=group('p99p100','US');
  $('#insights').innerHTML=insight('La distancia en el quintil superior',`El 20 % superior reúne <strong>${pct(es.share)}</strong> en España y <strong>${pct(us.share)}</strong> en EE. UU. La cuota estadounidense supera la española en <strong>${nfmt((us.share-es.share)*100,2)} puntos porcentuales</strong>.`)+insight('La distancia en el 1 %',`El 1 % superior posee <strong>${pct(ones.share)}</strong> en España y <strong>${pct(onus.share)}</strong> en EE. UU.: <strong>${nfmt((onus.share-ones.share)*100,2)} puntos porcentuales</strong> más en EE. UU. Estas cuotas describen concentración, no la cantidad total de riqueza del país.`);
  const ids=['p0p20','p20p40','p40p60','p60p80','p80p100','p0p50','p0p90','p99p100','p99.9p100','p99.99p100','p99.999p100'];
  table(['Grupo','España: cuota','EE. UU.: cuota','ES − EE. UU. (pp)','España: media','EE. UU.: media'],ids.map(id=>{const a=group(id,'ES'),b=group(id,'US');return {cells:[groupName(id),pct(a.share),pct(b.share),nfmt((a.share-b.share)*100,2),money(a.mean),money(b.mean)]};}),'Cuotas e importes compatibles · España / Estados Unidos');
  exportRows=[['grupo','ES_cuota','US_cuota','ES_menos_US_pp','ES_media_EUR_2025','US_media_EUR_2025'],...ids.map(id=>{const a=group(id,'ES'),b=group(id,'US');return [id,a.share,b.share,(a.share-b.share)*100,a.mean,b.mean];})];
}
function renderLevel() {
  setHead('06 / Cuánto patrimonio','La media no cuenta toda la historia','Media y mediana nacionales por adulto, en euros a precios de 2025.');
  const ns=data.countries.map(c=>({...c,n:nat(c.code)})).sort((a,b)=>b.n.median-a.n.median),max=Math.max(...ns.map(c=>c.n.mean))*1.04;
  $('#visual').innerHTML=`<div class="legend"><span><i class="dot" style="background:#fef702"></i>Media</span><span><i class="dot" style="background:#67b6df"></i>Mediana</span><span>Escala lineal común · EUR de 2025</span></div><div class="rankChart">${ns.map(c=>`<div class="rankRow ${c.code===state.country?'selected':''}"><div class="rankCountry">${esc(c.name)}</div><div style="display:grid;gap:5px"><div class="barTrack" style="height:12px" tabindex="0" data-tip="${esc(`${c.name}|Media: ${money(c.n.mean)}|Mediana: ${money(c.n.median)}`)}"><span class="bar" style="left:0;width:${c.n.mean/max*100}%;background:#fef702"></span></div><div class="barTrack" style="height:12px"><span class="bar" style="left:0;width:${Math.max(0,c.n.median)/max*100}%;background:#67b6df"></span></div></div><span style="font-size:10px;color:#bbb;text-align:right">${compact(c.n.mean)}<br>${compact(c.n.median)}</span></div>`).join('')}</div>`;
  setInternationalFoot('Orden descendente de mediana. Importes en EUR de 2025; no son euros ajustados por poder adquisitivo.');
  const n=nat();
  $('#insights').innerHTML=insight('La mediana sitúa al adulto central',`En ${countryName(state.country)}, la mediana es <strong>${money(n.median)}</strong>: la mitad de los adultos queda por debajo y la mitad por encima. La media asciende a <strong>${money(n.mean)}</strong>${n.median>0?', '+nfmt(n.mean/n.median,2)+' veces la mediana':''}.`)+insight('La media se mueve con las grandes fortunas','Al sumar todo el patrimonio y dividirlo entre los adultos, las grandes fortunas elevan la media. La mediana responde a otra pregunta: dónde está el centro de la distribución. Ninguna de las dos equivale al patrimonio de todos los integrantes de un grupo.');
  table(['País','Media por adulto','Mediana nacional','Media / mediana','Patrimonio nacional','Adultos estimados'],ns.map(c=>({selected:c.code===state.country,cells:[c.name,money(c.n.mean),money(c.n.median),c.n.median>0?nfmt(c.n.mean/c.n.median,2)+' ×':'No interpretable',compact(c.n.total),nfmt(c.n.population,0)]})),'Nivel de riqueza · 2024 · EUR de 2025');
  exportRows=[['pais','media_EUR_2025','mediana_EUR_2025','ratio_media_mediana','patrimonio_nacional_EUR_2025','adultos'],...ns.map(c=>[c.code,c.n.mean,c.n.median,c.n.median>0?c.n.mean/c.n.median:null,c.n.total,c.n.population])];
}

const dwaComponents=[['real_estate_total','Inmuebles'],['nonfinancial_business_assets_total','Activos empresariales no financieros'],['unlisted_shares_and_other_equity_total','Empresas: participaciones no cotizadas'],['listed_shares_total','Acciones cotizadas'],['investment_fund_shares_total','Fondos de inversión'],['deposits_total','Depósitos'],['debt_securities_total','Bonos'],['life_insurance_and_annuity_entitlements_total','Seguros de vida y rentas vitalicias'],['total_liabilities_total','Deudas']];
const usComponents=[['assets_real_estate','Inmuebles'],['assets_consumer_durables','Bienes duraderos'],['assets_equities_mutual_funds','Acciones y fondos'],['assets_deposits','Depósitos'],['assets_money_market_funds','Fondos monetarios'],['assets_debt_securities','Bonos'],['assets_loans','Préstamos: activos'],['assets_life_insurance','Seguros de vida'],['assets_annuities','Rentas vitalicias'],['assets_pensions_defined_contribution','Pensiones: contribución definida'],['assets_pensions_defined_benefit','Pensiones: prestación definida'],['assets_miscellaneous','Otros activos'],['liabilities_total','Deudas']];
function renderComposition() {
  const obs=data.official.filter(o=>o.country===state.country), isDwa=obs.some(o=>o.dataset_family==='europe_dwa'),isUS=state.country==='US';
  const publishedShares=obs.filter(o=>['component_share_of_net_wealth','component_share_of_financial_assets','component_share_of_real_assets'].includes(o.indicator));
  const sourceChoice=isDwa&&publishedShares.length?segment([['recent','DWA reciente'],['survey','Encuesta nacional']],state.compositionSource,'composition-source'):'';
  if(sourceChoice&&state.compositionSource==='survey'){
    setHead('07 / De qué está hecha la riqueza',`${countryName(state.country)}: activos y deudas`,'Composición publicada por la encuesta nacional.');
    renderPublishedComposition(publishedShares);
    $('#controls').insertAdjacentHTML('afterbegin',sourceChoice);
    return;
  }
  const family=isDwa?'europe_dwa':isUS?'us_canada':null;
  const relevant=family?obs.filter(o=>o.dataset_family===family):[];
  const periods=[...new Set(relevant.map(o=>o.period))].sort().reverse();
  const period=periods.find(p=>relevant.some(o=>o.period===p && (o.indicator==='total_assets_total'||o.indicator==='assets_total')));
  const current=relevant.filter(o=>o.period===period);
  const options=[...new Set(current.filter(o=>o.indicator==='total_assets_total'||o.indicator==='assets_total').map(o=>o.group))].sort((a,b)=>{const x=a.match(/[\d.]+/g).map(Number),y=b.match(/[\d.]+/g).map(Number);return x[0]-y[0]||y[1]-x[1];});
  if(!options.includes(state.compositionGroup))state.compositionGroup=options.includes('p0p50')?'p0p50':options[0];
  setHead('07 / De qué está hecha la riqueza',`${countryName(state.country)}: activos y deudas`,'Fuente nacional disponible. Las definiciones y los años pueden diferir de la comparación WID.',sourceChoice+(options.length?`<label class="field">Grupo patrimonial<select id="compositionGroup">${options.map(g=>`<option value="${g}" ${g===state.compositionGroup?'selected':''}>${groupName(g)}</option>`).join('')}</select></label>`:''));
  if(!period){
    if(['JP','KR','MX'].includes(state.country) && obs.some(o=>o.component && /asset_or_debt_component/.test(o.indicator))){renderNationalComponents(obs);return;}
    // Some national sources only publish composition as percentages, with a source-specific denominator.
    const shareObs=obs.filter(o=>['component_share_of_net_wealth','component_share_of_financial_assets','component_share_of_real_assets'].includes(o.indicator));
    if(shareObs.length){renderPublishedComposition(shareObs);return;}
    $('#visual').innerHTML=`<div class="empty"><strong>La recopilación no contiene un desglose completo y compatible de activos para esta vista.</strong><p>Los quintiles y las colas WID de 2024 no incluyen composición compatible. Consulta los registros nacionales descargables para conocer la cobertura de ${countryName(state.country)}.</p><a class="btn" href="data/estudio.json" download>Descargar los registros y sus definiciones ↓</a></div>`;
    $('#chartFoot').textContent='No se estima una composición a partir de cuotas de riqueza ni se combinan partidas con unidades o denominadores diferentes.';
    $('#insights').innerHTML=insight('La ausencia afecta a esta pregunta','Se puede estudiar cuánto patrimonio tiene cada grupo con WID 2024. Esta fuente no permite determinar qué parte de ese patrimonio es vivienda, empresas o activos financieros en todos los países.')+insight('Dos proporciones diferentes','La composición mide qué parte de los activos de un grupo es vivienda, por ejemplo. La propiedad nacional mide qué parte de todas las viviendas posee ese grupo. Sus denominadores son distintos.');
    $('#detailPanel').hidden=true;return;
  }
  const selected=current.filter(o=>o.group===state.compositionGroup),get=k=>selected.find(o=>o.indicator===k),total=get(isDwa?'total_assets_total':'assets_total');
  const components=(isDwa?dwaComponents:usComponents).map(([key,label])=>{const o=get(key);return o?{...o,label,signed:isUS&&key==='liabilities_total'?-o.value:o.value}:null;}).filter(Boolean);
  const denominator=total.value;
  const net=get('net_wealth_total');
  const negative=Math.min(0,...components.map(o=>o.signed/denominator*100)),range=100-negative,zero=-negative/range*100;
  $('#viewSubtitle').textContent=`${period} · ${groupName(state.compositionGroup)} · Hogares ordenados por patrimonio neto · ${isDwa?'BCE / SEBC, DWA':'Reserva Federal, DFA'}`;
  $('#visual').innerHTML=`<div class="officialBadge">Fuente oficial · ${esc(period)} · estimación publicada</div><div class="distributionHeader"><strong>Composición de los activos del grupo</strong><span>% de los activos brutos del propio grupo</span></div><div class="compositionRows">${components.map((o,i)=>{const v=o.signed/denominator*100,w=Math.abs(v)/range*100;return `<div class="compositionRow"><span>${o.label}</span><div class="barTrack" tabindex="0" data-tip="${esc(`${o.label}|${nfmt(v,2)} % de los activos del grupo|${nfmt(o.signed,0)} ${o.currency}|Registro: ${o.id}`)}"><span class="zero" style="left:${zero}%"></span><span class="bar" style="left:${v<0?zero-w:zero}%;width:${w}%;background:${v<0?'#f08b8b':colors[i%5]}"></span></div><span class="value ${v<0?'negative':''}">${nfmt(v,2)} %</span></div>`;}).join('')}</div><div class="compositionNote">Denominador: activos brutos del grupo (${nfmt(denominator,0)} ${esc(total.currency)}). Las deudas se muestran con signo negativo sobre ese mismo denominador. ${isDwa?'El concepto DWA excluye derechos sobre pensiones ocupacionales y públicas; la partida de seguros de vida no cubre todas las pensiones privadas.':'DFA incluye bienes duraderos y pensiones privadas. «Otros activos» conserva el agregado de la fuente; no se interpreta como una medida aislada de patrimonio empresarial.'}</div>`;
  $('#chartFoot').innerHTML=`${esc(countryName(state.country))} · ${esc(period)} · ${esc(total.statistical_unit)} · ${groupName(state.compositionGroup)}. Composición derivada: componente / activos brutos del grupo × 100. Importes nominales del periodo, en ${esc(total.currency)}. Fuente: ${sourceLink(total.source_reference)}. ${isDwa?'DWA publica estimaciones experimentales reconciliadas con cuentas nacionales.':'DFA combina encuestas y cuentas financieras.'} No comparar estas cifras directamente con adultos WID.`;
  const housing=components.find(o=>o.indicator===(isDwa?'real_estate_total':'assets_real_estate'));
  const financialKeys=isDwa?['listed_shares_total','investment_fund_shares_total','unlisted_shares_and_other_equity_total']:['assets_equities_mutual_funds'];
  const shareFinancial=components.filter(o=>financialKeys.includes(o.indicator)).reduce((s,o)=>s+o.value,0)/denominator;
  $('#insights').innerHTML=insight('El peso de la vivienda',`Los inmuebles representan <strong>${housing?pct(housing.value/denominator):'No disponible'}</strong> de los activos del ${groupName(state.compositionGroup).toLowerCase()}. ${isDwa?'La fuente agrupa vivienda habitual y otros inmuebles.':'Incluye la categoría inmobiliaria publicada en DFA.'}`)+insight('Participaciones y activos financieros',`${isDwa?'Acciones, fondos y participaciones empresariales no cotizadas':'Acciones y fondos'} reúnen <strong>${pct(shareFinancial)}</strong> de los activos brutos del grupo. El patrimonio neto del grupo es ${net?'<strong>'+nfmt(net.value,0)+' '+esc(net.currency)+'</strong>':'no disponible'}. Cambia de grupo para explorar el contraste.`);
  table(['Componente','Importe nominal del grupo','% de activos brutos','Registro de origen'],components.map(o=>({cells:[o.label,`${nfmt(o.signed,0)} ${o.currency}`,nfmt(o.signed/denominator*100,2)+' %',`<a href="${safeURL(o.source_url || src(o.source_reference)?.url)}" target="_blank" rel="noopener" class="trace">${esc(o.id)}</a>`]})),`Composición oficial · ${period} · ${groupName(state.compositionGroup)}`);
  exportRows=[['pais','periodo','grupo','componente','importe_nominal_con_signo','moneda','activos_brutos_denominador','porcentaje_activos','registro'],...components.map(o=>[o.country,o.period,o.group,o.indicator,o.signed,o.currency,denominator,o.signed/denominator*100,o.id])];
}
function renderPublishedComposition(obs) {
  const period=[...new Set(obs.map(o=>o.period))].sort().at(-1),current=obs.filter(o=>o.period===period), opts=[...new Set(current.map(o=>o.group))];
  if(!opts.includes(state.compositionGroup))state.compositionGroup=opts.includes('p0p100')?'p0p100':opts[0];
  $('#controls').innerHTML=`<label class="field">Grupo de la fuente<select id="compositionGroup">${opts.map(g=>`<option value="${g}" ${g===state.compositionGroup?'selected':''}>${groupName(g)}</option>`).join('')}</select></label>`;
  const selected=current.filter(o=>o.group===state.compositionGroup);
  const kindNames={component_share_of_net_wealth:'% del patrimonio neto del grupo',component_share_of_financial_assets:'% de los activos financieros del grupo',component_share_of_real_assets:'% de los activos reales del grupo'};
  $('#viewSubtitle').textContent=`${countryName(state.country)} · ${period} · ${groupName(state.compositionGroup)} · Composición publicada por la fuente`;
  $('#visual').innerHTML=`<div class="officialBadge">Fuente nacional · ${esc(period)}</div>${[...new Set(selected.map(o=>o.indicator))].map(ind=>`<h3 class="subheading">${kindNames[ind]}</h3><div class="compositionRows">${selected.filter(o=>o.indicator===ind).map((o,i)=>`<div class="compositionRow"><span>${esc(componentLabel(o.component))}</span><div class="barTrack"><span class="bar" style="left:0;width:${Math.min(100,Math.max(0,o.value))}%;background:${colors[i%5]}"></span></div><span class="value">${nfmt(o.value,1)} %</span></div>`).join('')}</div>`).join('')}<p class="caption">Categorías y denominadores originales. La cobertura puede ser parcial: no se completa hasta el 100 %. Se conservan las diferencias de redondeo y no se renormalizan las cuotas.</p>`;
  $('#chartFoot').innerHTML=`${esc(countryName(state.country))} · ${esc(period)} · Unidad: ${esc(selected[0]?.statistical_unit)}. Fuente: ${sourceLink(selected[0]?.source_reference)}. Cada bloque indica su propio denominador; las cuotas de activos reales y financieros no se suman entre sí.`;
  $('#insights').innerHTML=insight('Un desglose con su propia unidad','Estas cifras corresponden a la fuente nacional y al periodo indicado. Su unidad, cobertura de activos y clasificación de los grupos pueden diferir de los adultos equal-split de WID 2024.')+insight('Composición no equivale a propiedad nacional','Un 70 % de vivienda en los activos del grupo no significa que ese grupo posea el 70 % de las viviendas del país. El primer porcentaje describe su cartera; el segundo requeriría el total nacional del activo.');
  table(['Componente','Porcentaje','Denominador','Registro'],selected.map(o=>({cells:[esc(componentLabel(o.component)),nfmt(o.value,1)+' %',kindNames[o.indicator],esc(o.id)]})),`Composición publicada · ${period}`);
  exportRows=[['pais','periodo','grupo','componente','porcentaje','indicador_denominador','unidad_estadistica','registro'],...selected.map(o=>[o.country,o.period,o.group,o.component,o.value,o.indicator,o.statistical_unit,o.id])];
}
function componentLabel(s) {
  return ({main_residence:'Vivienda habitual',other_real_estate:'Otros inmuebles',self_employment_business:'Negocios por cuenta propia',jewellery_art_antiques:'Joyas, arte y antigüedades',deposits:'Depósitos',listed_shares:'Acciones cotizadas',investment_funds:'Fondos de inversión',pension_plans:'Planes de pensiones',life_insurance:'Seguros de vida',unlisted_shares:'Acciones no cotizadas',financial_wealth:'Patrimonio financiero',physical_wealth:'Patrimonio físico',property_wealth:'Patrimonio inmobiliario',private_pension_wealth:'Pensiones privadas',net_property_wealth:'Patrimonio inmobiliario neto',net_financial_wealth:'Patrimonio financiero neto',financial_assets:'Activos financieros',financial_savings:'Ahorro financiero',rental_deposits_paid_asset:'Depósitos de alquiler entregados',other_real_assets:'Otros activos reales',total_debt:'Deudas totales',total_financial_debt:'Deuda financiera total',main_residence_and_land:'Vivienda habitual y suelo',other_housing_and_land:'Otras viviendas y suelo',nonfinancial_assets:'Activos no financieros',mortgage_debt:'Deuda hipotecaria',nonmortgage_debt:'Otras deudas',bonds:'Bonos',mutual_funds:'Fondos de inversión',nontransaction_accounts_deposits:'Depósitos en cuentas no utilizables para pagos',other_financial_assets:'Otros activos financieros',pension_plans_investment_mixed_life_insurance:'Planes de pensiones y seguros de inversión o mixtos',transaction_accounts_deposits:'Depósitos en cuentas utilizables para pagos',unlisted_equity:'Participaciones no cotizadas'})[s] ?? s?.replaceAll('_',' ') ?? 'Componente';
}
function renderNationalComponents(obs) {
  const items=obs.filter(o=>/asset_or_debt_component/.test(o.indicator)),period=[...new Set(items.map(o=>o.period))].sort().at(-1),current=items.filter(o=>o.period===period),opts=[...new Set(current.map(o=>o.group))];
  if(!opts.includes(state.compositionGroup))state.compositionGroup=opts.includes('p0p100')?'p0p100':opts[0];
  $('#controls').innerHTML=`<label class="field">Grupo de la fuente<select id="compositionGroup">${opts.map(g=>`<option value="${g}" ${g===state.compositionGroup?'selected':''}>${groupName(g)}</option>`).join('')}</select></label>`;
  const selected=current.filter(o=>o.group===state.compositionGroup),get=k=>selected.find(o=>o.component===k);
  const keys=state.country==='JP'?['main_residence_and_land','other_housing_and_land','financial_assets','total_financial_debt']:state.country==='KR'?['main_residence','other_real_estate','other_real_assets','financial_savings','rental_deposits_paid_asset','total_debt']:['nonfinancial_assets','financial_assets','mortgage_debt','nonmortgage_debt'];
  const comps=keys.map(k=>get(k)).filter(Boolean).map(o=>({...o,signed:/debt/.test(o.component)?-Math.abs(o.value):o.value}));
  const denominator=state.country==='JP'?(get('financial_assets')?.value??0)+(get('residential_property_land')?.value??0):state.country==='KR'?get('gross_assets')?.value:comps.filter(o=>o.signed>0).reduce((s,o)=>s+o.signed,0);
  const low=Math.min(0,...comps.map(o=>o.signed/denominator*100)),range=100-low,zero=-low/range*100;
  $('#viewSubtitle').textContent=`${countryName(state.country)} · ${period} · ${groupName(state.compositionGroup)} · Hogares · ${state.country==='MX'?'importes agregados':'medias por hogar'}`;
  $('#visual').innerHTML=`<div class="officialBadge">Fuente nacional · ${esc(period)} · ${state.country==='MX'?'ENFIH':'encuesta de hogares'}</div><div class="distributionHeader"><strong>Composición del grupo</strong><span>% de sus activos brutos cubiertos</span></div><div class="compositionRows">${comps.map((o,i)=>{const v=o.signed/denominator*100,w=Math.abs(v)/range*100;return `<div class="compositionRow"><span>${componentLabel(o.component)}</span><div class="barTrack" data-tip="${esc(`${componentLabel(o.component)}|${nfmt(o.signed,0)} ${o.currency}|${nfmt(v,2)} % de los activos cubiertos|Registro: ${o.id}`)}" tabindex="0"><span class="zero" style="left:${zero}%"></span><span class="bar" style="left:${v<0?zero-w:zero}%;width:${w}%;background:${v<0?'#f08b8b':colors[i%5]}"></span></div><span class="value ${v<0?'negative':''}">${nfmt(v,2)} %</span></div>`;}).join('')}</div><div class="compositionNote">Denominador: ${nfmt(denominator,0)} ${esc(comps[0].currency)} ${state.country==='MX'?'de activos agregados':'de activos medios por hogar'}. ${state.country==='JP'?'La cobertura incluye activos financieros y vivienda/suelo residencial. Excluye otras categorías empresariales y bienes duraderos; los grupos de distribución excluyen hogares con patrimonio desconocido.':state.country==='KR'?'Los depósitos de alquiler entregados son activos; las obligaciones por depósitos recibidos forman parte de las deudas. Se evitan las sumas entre agregados y sus subcomponentes.':'Dato de 2019: es la cobertura histórica disponible de ENFIH. No se presenta como un dato de 2025–2026.'}</div>`;
  $('#chartFoot').innerHTML=`${esc(countryName(state.country))} · ${period} · ${esc(comps[0].statistical_unit)} · ${groupName(state.compositionGroup)}. ${state.country==='MX'?'Totales del grupo':'Medias por hogar'}, precios nominales del periodo, ${esc(comps[0].currency)}. Cuotas calculadas: componente / activos brutos cubiertos × 100. Fuente: ${sourceLink(comps[0].source_reference)}. Las deudas se presentan con signo negativo.`;
  $('#insights').innerHTML=insight('Una fuente y un denominador propios','El gráfico utiliza componentes compatibles de una misma fuente, periodo y grupo. Conserva la moneda local para mostrar los importes nominales de la encuesta. La composición describe la cartera del grupo, no su propiedad sobre un activo nacional.')+insight('Las categorías cambian entre países','Los componentes reflejan la cobertura de la encuesta nacional. Las categorías agregadas y sus subcomponentes no se suman entre sí. Esta vista complementa la distribución WID; sus grupos y cantidades no se trasladan a los adultos WID de 2024.');
  table(['Componente',state.country==='MX'?'Importe agregado':'Media por hogar','% de activos cubiertos','Registro'],comps.map(o=>({cells:[componentLabel(o.component),`${nfmt(o.signed,0)} ${o.currency}`,nfmt(o.signed/denominator*100,2)+' %',esc(o.id)]})),`Composición nacional · ${period}`);
  exportRows=[['pais','periodo','grupo','componente','valor_nominal_con_signo','moneda','denominador','porcentaje_activos','registro'],...comps.map(o=>[o.country,o.period,o.group,o.component,o.signed,o.currency,denominator,o.signed/denominator*100,o.id])];
}
function renderSources() {
  setHead('08 / Leer los datos','Fuentes, método y límites','La trazabilidad forma parte del estudio. Datos descargados el 18 de octubre de 2026.');
  const cov=data.coverage.filter(o=>o.country===state.country);
  const n25=nat(state.country,2025);
  $('#visual').innerHTML=`<div class="downloads"><a class="btn primary" href="data/estudio.json" download>Datos y trazabilidad · JSON ↓</a><a class="btn" href="data/grupos_2024.csv" download>Todos los grupos · CSV ↓</a><a class="btn" href="metodologia.pdf" target="_blank" rel="noopener">Metodología · PDF ↗</a><button class="btn" id="print">Imprimir estudio</button></div><div class="prose">
    <h3>Qué comparamos</h3><p>La base internacional contiene <strong>1.397 registros de grupo, 127 por país</strong>: quintiles, deciles, percentiles, grupos acumulados e intervalos exclusivos. Corresponden a <strong>2024</strong>. Los 100 percentiles son intervalos publicados; no se han interpolado quintiles. Las categorías se solapan y no deben sumarse entre sí.</p>
    <p>Unidad: <strong>adultos de 20 años o más, equal-split</strong>; el patrimonio de las parejas se reparte por igual según el concepto WID. Incluye activos financieros y no financieros menos deudas. Los activos de pensiones y seguros que incluye WID no equivalen a derechos futuros sobre pensiones públicas.</p>
    <h3>Grado 0: estimación publicada, no observación directa</h3><p>${esc(data.definitions.quality_0_distributed_wealth)} Las conclusiones describen las cifras del modelo publicado. El detalle de las colas superiores y los decimales no garantizan precisión empírica. El estudio no prueba por sí solo mecanismos de influencia política ni clasifica a cada integrante de un percentil como oligarca.</p>
    <h3>Fechas e importes</h3><p>El año del patrimonio (2024) difiere del año de precios (2025). Los importes se convierten a EUR con el tipo medio de mercado del BCE de 2025: <code>EUR = moneda local / unidades de moneda local por EUR</code>. No es una conversión por paridad de poder adquisitivo ni una actualización del dato a 2025.</p>
    <p>${n25?`Para ${countryName(state.country)}, el agregado nacional WID de <strong>2025</strong> es <strong>${compact(n25.total)}</strong>. La recopilación no contiene la distribución detallada equivalente de 2025; no aplicamos las cuotas de 2024 a ese agregado.`:''}</p>
    <h3>Cómo se calculan las cifras</h3><p>Cuota en % = cuota publicada × 100. Patrimonio total de grupo = media del grupo × población adulta × fracción de población. Adultos representados = población adulta × fracción. La mediana de un grupo es el umbral percentilar de su punto central, cuando está disponible. Diferencia entre cuotas = 100 × (cuota A − cuota B), en puntos porcentuales. Cuota dentro del 1 % = cuota del subgrupo / cuota del 1 %. Media respecto a mediana = media del grupo / mediana nacional, sólo si esta última es positiva.</p>
    <h3>Ceros, negativos y discrepancias</h3><p>Conservamos los negativos y los ceros publicados. Un grupo con media negativa no permite contar cuántos integrantes tienen patrimonio negativo. Los ceros redondeados no identifican personas con patrimonio exactamente cero. Las cuotas no se renormalizan; pequeñas diferencias entre sumas, medias y agregados se mantienen. En DWA existen comprobaciones de cuota del 10 % que exceden la tolerancia elegida: se conservan los indicadores originales y no se sustituyen unos por otros.</p>
    <h3>Datos oficiales recientes: una segunda lectura</h3><p>El JSON contiene <strong>4.921 observaciones oficiales</strong>. Hogares DWA/DFA, hogares privados ONS, familias económicas canadienses y las demás unidades nacionales conservan sus conceptos. La vista de composición indica periodo, fuente, unidad y denominador. Las fuentes nacionales no se combinan para generar una clasificación internacional de 2025–2026.</p>
    <details><summary>Cobertura 2025–2026 de ${esc(countryName(state.country))}</summary><pre>${esc(JSON.stringify(cov,null,2))}</pre></details>
    <details><summary>Definiciones originales de las fuentes nacionales</summary><pre>${esc(JSON.stringify(data.official_definitions,null,2))}</pre></details>
    <details><summary>Auditoría de ensamblaje y validaciones</summary><pre>${esc(JSON.stringify(data.validation,null,2))}</pre></details>
    <h3>De la distribución a la idea de oligarquía</h3><p>La referencia narrativa de Johnny Harris y Jeffrey Winters invita a estudiar cómo la concentración de patrimonio puede convertirse en capacidad para defenderlo e influir. Aquí medimos la distribución patrimonial. La influencia política, los mecanismos fiscales y las relaciones causales requieren evidencias adicionales y no se deducen de una cuota de riqueza.</p>
    <h3>Referencias visuales y de contexto</h3><p><a href="https://www.youtube.com/watch?v=2GxlL5-0m_g" target="_blank" rel="noopener">Politizane · Wealth Inequality in America (Updated 2026)</a>: secuencia de quintiles, cien representantes y ampliación de la cima. Se adopta esa lógica con las cifras de esta recopilación. <a href="https://www.youtube.com/watch?v=4S25FfbFw4M" target="_blank" rel="noopener">Johnny Harris · Oligarchy is worse than you think</a>: contexto conceptual. No se incorporan encuestas sobre el reparto percibido o ideal.</p>
    <h3>Fuentes originales de la recopilación</h3><ul class="sourceList">${data.sources.map(s=>`<li>${sourceLink(s.id)}<small>${esc(s.family)} · ${esc(s.country || 'Internacional')} · ${esc(s.retrieval_date || s.retrieved_at || 'Fecha no disponible')}</small></li>`).join('')}</ul>
    <details><summary>Reproducibilidad y origen del archivo</summary><p>El archivo web se genera con <code>build-data.mjs</code> a partir del JSON original. Conserva el valor numérico original, los identificadores de entradas, el estado y las fórmulas por métrica; las fuentes enlazan los datos originales publicados. SHA-256 del JSON de entrada: <code>${esc(data.source_sha256)}</code>.</p></details>
  </div>`;
  $('#chartFoot').textContent='Los datos enlazados corresponden a la descarga documentada. Las series de las fuentes externas pueden revisarse después de esa fecha.';
  $('#insights').innerHTML='';$('#detailPanel').hidden=true;
}
function render() {
  if(!data)return;
  $('#tooltip').hidden=true;
  document.querySelectorAll('.tabs button').forEach(b=>{const on=b.dataset.view===state.view;b.classList.toggle('on',on);on?b.setAttribute('aria-current','page'):b.removeAttribute('aria-current');});
  $('#countries').innerHTML=data.countries.map(c=>`<button class="country ${c.code===state.country?'on':''}" data-country="${c.code}" aria-pressed="${c.code===state.country}"><span class="countryCode">${c.code}</span><span>${esc(c.name)}</span><span class="arrow" aria-hidden="true">→</span></button>`).join('');
  $('#detailPanel .tracePanel')?.remove();
  renderStats();
  ({quintiles:renderQuintiles,personas:renderPeople,cima:renderTop,comparar:renderCompare,'espana-usa':renderPair,nivel:renderLevel,composicion:renderComposition,fuentes:renderSources})[state.view]();
  document.title=`${countryName(state.country)} · Oligarcas — N.Geolitics`;
}
function exportCSV() {
  if(!exportRows.length)return;
  const csv='\uFEFF'+exportRows.map(row=>row.map(v=>`"${String(v??'').replaceAll('"','""')}"`).join(';')).join('\r\n');
  const u=URL.createObjectURL(new Blob([csv],{type:'text/csv;charset=utf-8'})),a=document.createElement('a');
  a.href=u;a.download=`oligarcas_${state.country}_${state.view}_2024.csv`;a.click();setTimeout(()=>URL.revokeObjectURL(u),1000);
}
document.addEventListener('click',e=>{
  const el=e.target.closest('button,a');if(!el)return;
  if(el.dataset.view){e.preventDefault();activate(el.dataset.view);return;}
  if(el.dataset.country){state.country=el.dataset.country;history.replaceState(null,'',`#${state.view}?pais=${state.country}`);render();return;}
  if(el.dataset.metric){state.metric=el.dataset.metric;render();return;}
  if(el.dataset.scale){state.scale=el.dataset.scale;render();return;}
  if(el.dataset.compareStyle){state.compareStyle=el.dataset.compareStyle;render();return;}
  if(el.dataset.compositionSource){state.compositionSource=el.dataset.compositionSource;render();return;}
  if(el.id==='export')exportCSV();
  if(el.id==='print')window.print();
});
document.addEventListener('change',e=>{
  if(e.target.id==='ranking')state.ranking=e.target.value;
  else if(e.target.id==='percentileLimit')state.limit=+e.target.value;
  else if(e.target.id==='compositionGroup')state.compositionGroup=e.target.value;
  else return;
  render();
});
function showTip(el,x,y) {
  const t=$('#tooltip'),parts=el.dataset.tip.split('|');
  t.innerHTML=`<strong>${esc(parts[0])}</strong>${parts.slice(1).map(p=>`<small>${esc(p)}</small>`).join('')}`;t.hidden=false;
  const rect=t.getBoundingClientRect();t.style.left=Math.max(8,Math.min(x+14,window.innerWidth-rect.width-10))+'px';t.style.top=Math.max(8,Math.min(y+14,window.innerHeight-rect.height-10))+'px';
}
document.addEventListener('pointermove',e=>{const el=e.target.closest('[data-tip]');if(el)showTip(el,e.clientX,e.clientY);else $('#tooltip').hidden=true;});
document.addEventListener('focusin',e=>{const el=e.target.closest('[data-tip]');if(el){const r=el.getBoundingClientRect();showTip(el,r.x,r.y+r.height/2);}});
document.addEventListener('focusout',()=>$('#tooltip').hidden=true);
document.addEventListener('keydown',e=>{if(e.key==='Escape')$('#tooltip').hidden=true;});
window.addEventListener('scroll',()=>$('#tooltip').hidden=true,{passive:true});
window.addEventListener('hashchange',()=>readHash());
function readHash(){const [v,params]=location.hash.slice(1).split('?');const c=new URLSearchParams(params).get('pais');if(data.countries.some(x=>x.code===c))state.country=c;activate(v||'quintiles',false);}
async function init(){
  try{const response=await fetch('data/estudio.json');if(!response.ok)throw Error('HTTP '+response.status);data=await response.json();$('#status').hidden=true;$('#study').hidden=false;readHash();}
  catch(e){$('#status').innerHTML='No se han podido cargar los datos. <button class="btn" id="reload">Reintentar</button> <a href="data/grupos_2024.csv">Descargar CSV</a>';$('#reload').onclick=init;console.error(e);}
}
init();
