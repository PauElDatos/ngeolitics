'use strict';

window.QuintileDetail=(()=>{
  const colors=['#67b6df','#69c7ae','#bdcd73','#eeb55e','#fef702'];
  const escape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const percentile=p=>new Intl.NumberFormat('es-ES',{maximumFractionDigits:3}).format(p);
  let active;

  // Thresholds, rather than bin averages, describe the published boundaries of
  // the distribution. P100 is deliberately absent when its value is unknown.
  function thresholds(group,percentiles,cumulative){
    const points=new Map();
    for(const entry of [...percentiles,...cumulative]){
      if(entry.a>=group.a&&entry.a<group.b&&Number.isFinite(entry.lower))points.set(entry.a,entry.lower);
    }
    if(Number.isFinite(group.lower))points.set(group.a,group.lower);
    if(Number.isFinite(group.upper))points.set(group.b,group.upper);
    return [...points].sort((a,b)=>a[0]-b[0]).map(([p,value])=>({p,value}));
  }

  function dispose(){
    active?.abort();active=null;
    const dialog=document.querySelector('#quintileDetailDialog');
    if(dialog?.open)dialog.close();
    dialog?.remove();
  }

  function mount(root,context){
    dispose();
    const {name,country,quintiles,percentiles,cumulative,richestMan,richestSource,formatMoney,formatCompact,formatPercent,formatNumber,sourceHTML}=context;
    active=new AbortController();
    const {signal}=active;
    const dialog=document.createElement('dialog');
    dialog.id='quintileDetailDialog';dialog.className='quintileDialog';
    dialog.setAttribute('aria-labelledby','quintileDetailTitle');
    dialog.innerHTML=`<header class="quintileDialogHeader"><div><p class="quintileDialogCountry">${escape(name)} · 2024 · patrimonio por adulto</p><h2 id="quintileDetailTitle"></h2></div><button class="btn quintileDialogClose" type="button" aria-label="Cerrar el detalle del quintil">Cerrar ×</button></header><nav class="quintileDialogTabs" aria-label="Explorar otro quintil">${quintiles.map((q,i)=>`<button type="button" data-quintile-select="${escape(q.group)}" aria-pressed="false">Q${i+1}</button>`).join('')}</nav><div class="quintileDialogBody"></div>`;
    root.append(dialog);
    let selected,points=[],opener;
    const body=dialog.querySelector('.quintileDialogBody');

    function place(){
      if(!dialog.open)return;
      const viewportWidth=document.documentElement.clientWidth,viewportHeight=window.innerHeight;
      const box=(root.querySelector('.mobileScenePlot')??root.querySelector('.wealthSceneWrap')).getBoundingClientRect();
      const visibleTop=Math.max(16,box.top),visibleBottom=Math.min(viewportHeight-16,box.bottom);
      const nearGraph=visibleBottom-visibleTop>=240;
      const availableWidth=box.width>=440?Math.min(box.width-32,850,viewportWidth-32):viewportWidth-24;
      dialog.style.width=availableWidth+'px';
      dialog.style.maxHeight=(nearGraph?Math.min(viewportHeight-32,visibleBottom-visibleTop-16):viewportHeight-32)+'px';
      dialog.style.left=Math.max(availableWidth/2+12,Math.min(viewportWidth-availableWidth/2-12,box.left+box.width/2))+'px';
      dialog.style.top=(nearGraph?(visibleTop+visibleBottom)/2:viewportHeight/2)+'px';
    }

    function draw(){
      const chart=body.querySelector('.quintileCurve');
      if(!chart||!points.length)return;
      const width=Math.max(240,chart.clientWidth),height=230;
      const left=width<420?63:83,right=width-18,top=18,bottom=height-44;
      const values=[...points.map(p=>p.value),selected.mean].filter(Number.isFinite);
      const min=Math.min(0,...values),max=Math.max(0,...values),range=max-min||1;
      const raw=range/4,power=10**Math.floor(Math.log10(raw));
      const step=[1,2,5,10].find(n=>n*power>=raw)*power;
      const low=Math.floor(min/step)*step,high=Math.ceil(max/step)*step||step;
      const x=p=>left+(p-selected.a)/(selected.b-selected.a)*(right-left);
      const y=value=>bottom-(value-low)/(high-low)*(bottom-top);
      let grid='';
      for(let value=low;value<=high;value+=step){
        grid+=`<line x1="${left}" x2="${right}" y1="${y(value)}" y2="${y(value)}" class="quintileCurveGrid"/><text x="${left-8}" y="${y(value)+4}" text-anchor="end">${escape(formatCompact(value))}</text>`;
      }
      const xLabels=Array.from({length:5},(_,i)=>{
        const p=selected.a+(selected.b-selected.a)*i/4;
        return `<text x="${x(p)}" y="${bottom+21}" text-anchor="${i===0?'start':i===4?'end':'middle'}">P${percentile(p)}</text>`;
      }).join('');
      const mean=Number.isFinite(selected.mean)?`<line class="quintileCurveMean" data-value="${selected.mean}" x1="${left}" x2="${right}" y1="${y(selected.mean)}" y2="${y(selected.mean)}"/>`:'';
      const curve=points.map(point=>`${x(point.p)},${y(point.value)}`).join(' ');
      const dots=points.map(point=>`<circle cx="${x(point.p)}" cy="${y(point.value)}" r="2.8" data-percentile="${point.p}" data-value="${point.value}"/>`).join('');
      const last=points.at(-1);
      const openEnd=selected.upper==null?`<path class="quintileCurveOpen" d="M${x(last.p)} ${y(last.value)-6}V${Math.max(8,y(last.value)-20)}m-4 4 4-4 4 4"/>`:'';
      chart.innerHTML=`<svg viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" role="img" aria-label="Umbrales de patrimonio de Q${quintiles.indexOf(selected)+1}, del percentil ${percentile(selected.a)} al ${percentile(selected.b)}. Escala lineal. La línea discontinua indica el promedio.">${grid}${mean}<polyline class="quintileCurveLine" points="${curve}"/>${dots}${openEnd}${xLabels}<text x="${(left+right)/2}" y="${height-4}" text-anchor="middle">Percentil nacional de patrimonio</text></svg>`;
    }

    function metric(label,value,caption,key,extra=''){
      const available=Number.isFinite(value);
      return `<article class="quintileMetric ${extra}" data-metric="${key}" data-value="${available?value:''}"><h3>${label}</h3><strong>${available?escape(formatMoney(value)):key==='upper'?'Sin máximo publicado':'No disponible'}</strong><p>${caption}</p></article>`;
    }

    function show(id,trigger){
      const q=quintiles.find(group=>group.group===id);
      if(!q)return;
      selected=q;points=thresholds(q,percentiles,cumulative);if(trigger)opener=trigger;
      const index=quintiles.indexOf(q);
      dialog.style.setProperty('--quintile-color',colors[index]);
      dialog.dataset.country=country;dialog.dataset.group=q.group;
      dialog.querySelector('#quintileDetailTitle').textContent=`Dentro de Q${index+1} · ${q.a}–${q.b} %`;
      dialog.querySelectorAll('[data-quintile-select]').forEach(button=>button.setAttribute('aria-pressed',String(button.dataset.quintileSelect===q.group)));
      root.querySelectorAll('[data-quintile-detail]').forEach(button=>button.setAttribute('aria-expanded',String(button.dataset.quintileDetail===q.group)));
      const tail=q.b===100?cumulative.filter(group=>group.a>=99&&group.a<100).sort((a,b)=>a.a-b.a):[];
      const milestones=tail.length?`<section class="quintileMilestones" aria-label="Umbrales dentro de la parte más rica">${tail.map(group=>`<div><span>Entrada al ${percentile(group.fraction*100)} % superior</span><strong>${escape(formatCompact(group.lower))}</strong></div>`).join('')}</section>`:'';
      const highest=tail.at(-1);
      const spread=highest&&q.lower>0?`El umbral del ${percentile(highest.fraction*100)} % superior es <strong>${escape(formatNumber(highest.lower/q.lower))} veces</strong> el umbral de entrada a Q5.`:Number.isFinite(q.upper)&&Number.isFinite(q.lower)?`Entre los dos límites hay <strong>${escape(formatMoney(q.upper-q.lower))}</strong> de diferencia.`:'';
      const richest=q.b===100&&richestMan?`<aside class="quintileForbes"><div><span>Referencia de la cima · Forbes 2026</span><strong>${escape(richestMan.name)}${richestMan.includes_family?' y familia':''}</strong></div><div><strong>≈ ${escape(formatCompact(richestMan.usd_billions*1e9/richestSource.usd_per_eur))}</strong><span>${escape(formatNumber(richestMan.usd_billions*1000))} M$ originales</span></div><p>Estimación individual del 1-3-2026; conversión a EUR nominales con el BCE del 27-2-2026. Se muestra aparte de la curva WID de 2024 en EUR de 2025. <a href="${escape(richestSource.forbes_url)}" target="_blank" rel="noopener">Forbes ↗</a> · <a href="${escape(richestSource.fx_url)}" target="_blank" rel="noopener">BCE ↗</a></p></aside>`:'';
      body.innerHTML=`<p class="quintileDialogSummary">≈ ${escape(formatNumber(q.count))} adultos · ${escape(formatPercent(q.share))} de la riqueza nacional</p>
        <section class="quintileMetrics" aria-label="Límites y promedio del quintil">
          ${metric('Mínimo · umbral de entrada',q.lower,`Límite de P${percentile(q.a)}`,'lower')}
          ${metric('Promedio por adulto',q.mean,`Media de todo Q${index+1}`,'mean','quintileMetricMean')}
          ${metric(q.upper==null?'Máximo individual':'Máximo · límite superior',q.upper,q.upper==null?'La cola superior de WID queda abierta.':`Límite de P${percentile(q.b)}`,'upper')}
        </section>
        <section class="quintileCurvePanel"><div class="quintileCurveHeading"><h3>La brecha dentro del quintil</h3><span>Escala lineal</span></div><div class="quintileCurve"></div><div class="quintileCurveLegend"><span><i></i>Umbrales publicados</span><span><i class="quintileMeanKey"></i>Promedio de Q${index+1}</span></div></section>
        ${milestones}${spread?`<p class="quintileSpread">${spread}</p>`:''}${richest}
        <p class="quintileSourceNote">Los límites son umbrales percentilares; no máximos y mínimos individuales observados. ${q.upper==null?'La curva llega al último umbral disponible, P'+percentile(points.at(-1)?.p)+'. ':''}WID 2024 · EUR de 2025. Fuente: ${sourceHTML}</p>`;
      if(!dialog.open)dialog.showModal();
      dialog.scrollTop=0;place();draw();
      const tooltip=document.querySelector('#tooltip');if(tooltip)tooltip.hidden=true;
    }

    root.addEventListener('click',event=>{
      const trigger=event.target.closest('[data-quintile-detail]');
      if(trigger){event.preventDefault();show(trigger.dataset.quintileDetail,trigger);return;}
      const tab=event.target.closest('[data-quintile-select]');
      if(tab){event.preventDefault();show(tab.dataset.quintileSelect);}
    },{signal});
    root.addEventListener('keydown',event=>{
      if(event.target.matches('.wealth-quintile')&&['Enter',' '].includes(event.key)){
        event.preventDefault();show(event.target.dataset.quintileDetail,event.target);
      }
    },{signal});
    dialog.querySelector('.quintileDialogClose').addEventListener('click',()=>dialog.close(),{signal});
    dialog.addEventListener('click',event=>{
      if(event.target!==dialog)return;
      const box=dialog.getBoundingClientRect();
      if(event.clientX<box.left||event.clientX>box.right||event.clientY<box.top||event.clientY>box.bottom)dialog.close();
    },{signal});
    dialog.addEventListener('close',()=>{
      root.querySelectorAll('[data-quintile-detail]').forEach(button=>button.setAttribute('aria-expanded','false'));
      if(opener?.isConnected)opener.focus({preventScroll:true});
    },{signal});
    window.addEventListener('resize',()=>{if(dialog.open){place();draw();}},{signal});
    function insideDialog(event){
      const point=event.touches?.[0]??event,box=dialog.getBoundingClientRect();
      return dialog.contains(event.target)&&point.clientX>=box.left&&point.clientX<=box.right&&point.clientY>=box.top&&point.clientY<=box.bottom;
    }
    window.addEventListener('wheel',event=>{
      if(dialog.open&&!insideDialog(event)&&!event.ctrlKey)event.preventDefault();
    },{signal,passive:false});
    window.addEventListener('touchmove',event=>{
      if(dialog.open&&event.touches.length===1&&!insideDialog(event))event.preventDefault();
    },{signal,passive:false});
  }
  return {mount,dispose,thresholds};
})();
