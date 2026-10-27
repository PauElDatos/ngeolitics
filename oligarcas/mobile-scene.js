'use strict';

// Enhance only the phone layout. Desktop keeps the original scene and controls.
window.MobileWealthScene=(()=>{
  const phone=window.matchMedia('(max-width: 620px), (max-width: 900px) and (hover: none) and (pointer: coarse)');
  const colors=['#67b6df','#69c7ae','#bdcd73','#eeb55e','#fef702'];
  const escape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

  function mount(root,{name,population,quintiles,top,richestMan,richestSource,formatMoney,formatCompact,formatPercent,formatNumber}){
    if(!phone.matches)return false;
    const wrap=root.querySelector('.wealthSceneWrap'),svg=root.querySelector('#wealthGraphic');
    if(!wrap||svg?.dataset.kind!=='national')return false;
    const modes=root.querySelector('.sceneViewControls'),navigation=root.querySelector('.fixedSceneNavigation');
    const stats=root.querySelector('#stats'),reference=root.querySelector('.blockReference');
    const layout=document.createElement('div');
    layout.className='mobileWealthScene';
    const richestEUR=richestMan?richestMan.usd_billions*1e9/richestSource.usd_per_eur:0;
    layout.innerHTML=`
      <header class="mobileSceneHeading"><h2>${escape(name)}</h2><p>${escape(formatNumber(population))} adultos de 20 años o más · 2024</p></header>
      <div class="mobileSceneModes"></div>
      <div class="mobileSceneTools"><button class="btn mobileSceneZoom" type="button" aria-pressed="false">Ampliar bloques</button></div>
      <div class="mobileScenePlot"><div class="mobileSceneAxis" aria-hidden="true"></div></div>
      <p class="mobileSceneHint">Todo el reparto de izquierda a derecha. Toca una barra para ver sus datos.</p>
      <section class="mobileQuintiles" aria-label="Población y riqueza de los cinco quintiles"><h3>El reparto por quintiles</h3>
        ${quintiles.map((g,i)=>`<button class="mobileQuintileRow" type="button" style="--quintile-color:${colors[i]}" data-quintile-detail="${escape(g.group)}" aria-haspopup="dialog" aria-expanded="false" aria-controls="quintileDetailDialog"><span class="mobileQuintileName">Q${i+1}<small>${g.a}–${g.b} %</small></span><span class="mobileQuintileAdults">≈ ${escape(formatNumber(g.count))}<small>adultos estimados</small></span><span class="mobileQuintileShare">${escape(formatPercent(g.share))}<small>riqueza acumulada</small></span><svg class="mobileQuintileArrow" viewBox="0 0 16 16" aria-hidden="true"><path d="M3 10L8 5L13 10" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg></button>`).join('')}
      </section>
      <section class="mobileSceneHighlights" aria-label="El uno por ciento y la fortuna individual">
        <article><h3>El 1 % superior</h3><strong>${escape(formatPercent(top.share))}</strong><p>de la riqueza nacional</p><small>≈ ${escape(formatNumber(top.count))} adultos<br>${escape(formatCompact(top.mean))} por adulto</small></article>
        ${richestMan?`<article class="mobileRichest"><h3>${escape(richestMan.name)}</h3><strong>${escape(formatNumber(richestMan.usd_billions*1000))} M$</strong><p>≈ ${escape(formatCompact(richestEUR))}</p><small>Forbes 2026 · una persona${richestMan.includes_family?' y familia':''}</small></article>`:''}
      </section>
      <details class="mobileSceneLegend"><summary>Leyenda · cada bloque = 10.000 €</summary></details>
      <dialog class="mobileSceneDetails" aria-label="Datos del grupo seleccionado"><button class="btn mobileSceneClose" type="button" aria-label="Cerrar los datos">Cerrar ×</button><div class="mobileSceneDetailContent"></div></dialog>`;
    modes.querySelector('[data-scene-mode="split"]').textContent='1 % separado';
    modes.querySelector('[data-scene-mode="full"]').textContent='Todos juntos';
    modes.querySelector('[data-view="cima"]').textContent='Dentro del 1 % →';
    navigation.querySelector('[data-scene-scroll="top"]').textContent='↑ Cima';
    navigation.querySelector('[data-scene-scroll="base"]').textContent='↓ Base';
    layout.querySelector('.mobileSceneModes').append(modes);
    layout.querySelector('.mobileSceneTools').append(navigation);
    layout.querySelector('.mobileScenePlot').append(wrap);
    layout.querySelector('.mobileSceneLegend').append(reference);
    layout.querySelector('.mobileSceneLegend').before(stats);
    root.replaceChildren(layout);
    wrap.setAttribute('aria-label','Reparto del patrimonio: toca una barra para consultar sus datos o arrastra para explorar la altura.');
    // Replace the scaled SVG captions with readable HTML; all monetary geometry
    // and the shared green/gold block scale are retained from the original SVG.
    svg.querySelectorAll('#wealthSceneContent > line').forEach(line=>{
      if(['#43483f','#7b8177','#704e4b'].includes(line.getAttribute('stroke')))line.style.display='none';
    });
    const axis=layout.querySelector('.mobileSceneAxis'),zoomButton=layout.querySelector('.mobileSceneZoom');
    const hint=layout.querySelector('.mobileSceneHint');
    let expanded=false;
    function paintAxis({zoom,top:windowTop,zero,viewport,pixels,unit}){
      const span=viewport/5*unit,raw=span/4,magnitude=10**Math.floor(Math.log10(raw));
      const step=[1,2,5,10].find(factor=>factor*magnitude>=raw)*magnitude;
      const low=(zero-windowTop-viewport)/5*unit,high=(zero-windowTop)/5*unit;
      const values=[];
      for(let value=Math.ceil(low/step)*step;value<=high;value+=step)values.push(value);
      const lower=Number(svg.dataset.lower);
      if(lower<0&&Math.abs(lower)/unit*5*zoom>=28&&!values.includes(lower))values.push(lower);
      let html='';
      for(const value of values){
        const y=(zero-value/unit*5-windowTop)*zoom;
        if(y<12||y>pixels-12)continue;
        html+=`<div class="mobileAxisTick${value<0?' negative':''}${value===0?' mobileAxisZero':''}" style="top:${y}px"><span>${escape(formatCompact(value))}</span><i></i></div>`;
      }
      axis.innerHTML=html;
    }
    const bottom=Number(svg.dataset.zero)+Math.abs(Number(svg.dataset.lower))/Number(svg.dataset.blockUnit)*5+80;
    const controller=window.WealthScene.mount(root,{
      bottom,scale:(viewport,width)=>expanded?1:viewport.clientWidth/width,onPaint:paintAxis,
    });
    zoomButton.addEventListener('click',()=>{
      expanded=!expanded;
      zoomButton.textContent=expanded?'Ver reparto completo':'Ampliar bloques';
      zoomButton.setAttribute('aria-pressed',String(expanded));
      layout.classList.toggle('mobileSceneExpanded',expanded);
      hint.textContent=expanded?'Arrastra en horizontal para recorrer los bloques y en vertical para explorar la altura. Toca una barra para ver sus datos.':'Todo el reparto de izquierda a derecha. Toca una barra para ver sus datos.';
      wrap.scrollLeft=0;controller.refresh();
    });
    const dialog=layout.querySelector('.mobileSceneDetails');
    function showDetails(text){
      const parts=text.split('|');
      layout.querySelector('.mobileSceneDetailContent').innerHTML=`<h3>${escape(parts[0])}</h3>${parts.slice(1).map(part=>`<p>${escape(part)}</p>`).join('')}`;
      if(!dialog.open)dialog.showModal();
    }
    layout.querySelector('.mobileSceneClose').addEventListener('click',()=>dialog.close());
    dialog.addEventListener('click',event=>{if(event.target===dialog){const rect=dialog.getBoundingClientRect();if(event.clientY<rect.top)dialog.close();}});
    layout.addEventListener('click',event=>{
      const row=event.target.closest('[data-mobile-tip]');
      if(row)showDetails(row.dataset.mobileTip);
      else if(event.pointerType!=='touch'){
        const bar=event.target.closest('[data-tip]');
        if(bar)showDetails(bar.dataset.tip);
      }
    });
    // Native scrollbars cover an intentionally capped virtual height. Finger
    // movements must use screen distances, rather than that scrollbar's ratio.
    let drag=null;
    wrap.addEventListener('pointerdown',event=>{
      if(event.pointerType==='mouse')return;
      if(drag){drag=null;return;}
      drag={id:event.pointerId,x:event.clientX,y:event.clientY,startX:event.clientX,startY:event.clientY,moved:false,tip:event.target.closest('[data-tip]')?.dataset.tip};
    });
    wrap.addEventListener('pointermove',event=>{
      if(!drag||event.pointerId!==drag.id)return;
      event.stopPropagation();
      const dx=drag.x-event.clientX,dy=drag.y-event.clientY;
      if(Math.hypot(event.clientX-drag.startX,event.clientY-drag.startY)>7)drag.moved=true;
      if(drag.moved){
        wrap.setPointerCapture(event.pointerId);
        controller.pan(dx,dy);
      }
      drag.x=event.clientX;drag.y=event.clientY;
    });
    wrap.addEventListener('pointerup',event=>{
      if(!drag||event.pointerId!==drag.id)return;
      const tapped=!drag.moved&&drag.tip;drag=null;
      if(tapped)showDetails(tapped);
    });
    wrap.addEventListener('pointercancel',()=>{drag=null;});
    const country=document.querySelector('#countries .country.on'),countries=country?.parentElement;
    if(country&&countries)countries.scrollLeft=country.offsetLeft-countries.offsetLeft-(countries.clientWidth-country.offsetWidth)/2;
    return true;
  }
  return {mount};
})();
