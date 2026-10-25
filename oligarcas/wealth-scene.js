'use strict';

window.WealthScene = (() => {
  const palette=['#67b6df','#69c7ae','#bdcd73','#eeb55e','#fef702'];
  const ROWS=100,DX=10,DY=5,L=92;
  const escape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const precise=v=>new Intl.NumberFormat('es-ES',{minimumFractionDigits:2,maximumFractionDigits:2}).format(v)+' €';
  const fraction=v=>new Intl.NumberFormat('es-ES',{maximumFractionDigits:3}).format(v*100)+' %';

  // Full columns have 100 complete blocks. Only the last column has a remainder.
  function decompose(value,unit) {
    const magnitude=Math.abs(value),capacity=unit*ROWS;
    const blocks=Math.floor(magnitude/unit),remainder=magnitude-blocks*unit;
    const fullColumns=Math.floor(blocks/ROWS),lastBlocks=blocks%ROWS;
    const columns=Array.from({length:fullColumns},()=>({blocks:ROWS,remainder:0,value:capacity}));
    if(lastBlocks||remainder)columns.push({blocks:lastBlocks,remainder,value:lastBlocks*unit+remainder});
    return {blocks,remainder,fullColumns,columns,negative:value<0,capacity};
  }
  function packing(value,unit,formatNumber,formatMoney,single=false) {
    const p=decompose(value,unit);
    if(single)return `Una sola barra vertical: ${formatNumber(p.blocks)} bloques enteros de ${formatMoney(unit)}${p.remainder?' + resto '+precise(p.remainder):''}. Conserva el importe de las ${p.columns.length} columnas laterales`;
    return `${formatNumber(p.fullColumns)} columnas completas de ${formatMoney(p.capacity)}${p.columns.length>p.fullColumns?' + una última con '+precise(p.columns.at(-1).value):''}. ${formatNumber(p.blocks)} bloques enteros de ${formatMoney(unit)}${p.remainder?' + resto '+precise(p.remainder):''}${p.negative?' (deuda neta)':''}`;
  }
  function pile(g,x,zero,unit,tip,top=false,single=false) {
    const p=decompose(g.mean,unit),sign=p.negative?-1:1;
    const parts=single&&p.columns.length?[{blocks:p.blocks,remainder:p.remainder,value:Math.abs(g.mean)}]:p.columns;
    const columns=parts.map((c,i)=>{
      const height=c.blocks*DY,yy=p.negative?0:-height;
      // Exact multiples of the pattern period prevent cropped banknotes.
      return `<g class="wealth-stack-column" data-value="${sign*c.value}" data-blocks="${c.blocks}" data-remainder="${c.remainder}" transform="translate(${x+i*DX},${zero})"><rect class="${top?'wealth-top-column':'wealth-column'}" data-group="${escape(g.group)}" data-value="${sign*c.value}" x="0" y="${yy}" width="${DX}" height="${height}" fill="url(#${p.negative?'wealth-debt':'wealth-cash'})"/>${c.remainder?`<rect class="wealth-remainder" data-value="${sign*c.remainder}" data-unit-fraction="${c.remainder/unit}" x="1" y="${p.negative?height+.5:-height-DY+.5}" width="8" height="${DY-1}" fill="url(#wealth-rest)" stroke="#f3b865" stroke-width=".6"/>`:''}</g>`;
    }).join('');
    const width=Math.max(1,parts.length)*DX,hitRows=single?Math.max(ROWS,Math.ceil(Math.abs(g.mean)/unit)):ROWS;
    return `<g class="wealth-interval wealth-pile${top?' wealth-top':''}" data-layout="${single?'single':'wrapped'}" data-group="${escape(g.group)}" data-count="${g.count}" data-value="${g.mean}" data-block-count="${p.blocks}" data-remainder="${p.remainder}" data-full-columns="${p.fullColumns}" data-column-count="${parts.length}" tabindex="0" role="img" aria-label="${escape(tip.replaceAll('|','. '))}" data-tip="${escape(tip)}">${columns}<rect class="wealth-hit" x="${x}" y="${zero-hitRows*DY}" width="${width}" height="${hitRows*DY+Math.max(20,p.negative?Math.ceil(Math.abs(g.mean)/unit)*DY:20)}" fill="transparent"/></g>`;
  }
  function defs() {
    return `<defs><pattern id="wealth-cash" patternUnits="userSpaceOnUse" width="${DX}" height="${DY}"><rect x="1" y=".5" width="8" height="${DY-1}" rx=".25" fill="#7fab63"/><path d="M2 1.5H8" stroke="#bdd4a5" stroke-width=".45" opacity=".6"/></pattern><pattern id="wealth-debt" patternUnits="userSpaceOnUse" width="${DX}" height="${DY}"><rect x="1" y=".5" width="8" height="${DY-1}" fill="#dc8c82"/></pattern><pattern id="wealth-rest" patternUnits="userSpaceOnUse" width="3" height="3"><path d="M-1 1L1-1M0 3L3 0M2 4L4 2" stroke="#f3b865" stroke-width=".7"/></pattern><style>.wealth-pile:focus{outline:none}.wealth-pile:focus .wealth-hit,.wealth-pile:hover .wealth-hit{stroke:#fef702;stroke-width:.8}.wealth-column,.wealth-top-column{shape-rendering:crispEdges}</style></defs>`;
  }
  function grid(width,zero,unit,debtRows,formatCompact,positiveRows=ROWS) {
    let html='';
    for(let i=0;i<=5;i++){
      const blocks=positiveRows*i/5,y=zero-blocks*DY;
      html+=`<line x1="${L}" x2="${width-32}" y1="${y}" y2="${y}" stroke="${i?'#43483f':'#7b8177'}" stroke-width="${i?.8:1.2}"/><text x="${L-12}" y="${y+4}" fill="#a1a59c" text-anchor="end" font-size="12">${escape(formatCompact(blocks*unit))}</text>`;
    }
    if(debtRows)html+=`<line x1="${L}" x2="${width-32}" y1="${zero+debtRows*DY}" y2="${zero+debtRows*DY}" stroke="#704e4b" stroke-dasharray="3 5"/><text x="${L-12}" y="${zero+debtRows*DY+4}" fill="#dc9991" text-anchor="end" font-size="11">−${escape(formatCompact(debtRows*unit))}</text>`;
    return html;
  }
  function map(silhouette,T,width) {
    return silhouette?`<svg x="${L+100}" y="${T+20}" width="${Math.min(1050,width-L-150)}" height="${ROWS*DY-20}" viewBox="0 0 1000 500" preserveAspectRatio="xMidYMid meet" aria-hidden="true"><path d="${escape(silhouette.d)}" fill="#b1b8a2" opacity=".085"/></svg>`:'';
  }
  function frame({width,height,name,population,unit,T,zero,debtRows,silhouette,formatCompact,attributes,header,body,description,positiveRows=ROWS}) {
    return `<svg id="wealthGraphic" class="moneyScene fixedBlockScene" xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" style="--scene-width:${width}px" data-population="${population}" data-block-unit="${unit}" data-stack-capacity="${unit*ROWS}" data-block-rows="${ROWS}" data-plot-rows="${positiveRows}" data-block-width="8" data-block-height="${DY-1}" data-row-step="${DY}" data-upper="${unit*positiveRows}" data-lower="${-debtRows*unit}" ${attributes} role="group" aria-label="${escape(name)}: patrimonio en bloques monetarios de valor fijo" font-family="Arial, sans-serif"><title>${escape(name)}: bloques de ${escape(precise(unit))}</title><desc>${escape(description)} Todos los países y vistas comparten la unidad elegida manualmente. Las columnas laterales completas contienen 100 bloques; en «Todos juntos», el 1 % se reúne en una sola barra vertical sin ese límite de altura. Las marcas naranjas son restos menores que un bloque, no unidades completas.</desc>${defs()}<rect width="${width}" height="${height}" fill="#252825"/>${map(silhouette,zero-ROWS*DY,width)}${grid(width,zero,unit,debtRows,formatCompact,positiveRows)}${header}${body}</svg>`;
  }
  function reference(unit,formatMoney) {
    return `<div class="blockReference"><strong><i class="cashKey"></i>1 bloque entero = ${escape(formatMoney(unit))}</strong><span>100 bloques por columna lateral = ${escape(formatMoney(unit*ROWS))}</span><span class="blockRemainderKey"><i></i>Marca naranja = resto menor que un bloque, importe en el detalle</span><small>EUR de 2025 por adulto · Misma unidad en todos los países y vistas · Sin ajuste automático</small></div>`;
  }
  function wrap(svg,unit,formatMoney,note,button,joined=false,joinedValue='') {
    return reference(unit,formatMoney)+`<div class="fixedSceneNavigation"><p class="fixedSceneHint">${joined?'Todos juntos: el 1 % forma una sola barra ('+escape(joinedValue)+' por adulto), apilada hacia arriba con los mismos bloques. Desplázate ↕ para ver su altura completa.':'Desplaza la gráfica ↔: el dinero que supera una columna continúa en la siguiente. Las pilas no se encogen para caber.'}</p><button class="btn" data-scene-scroll="start">← Inicio</button><button class="btn" data-scene-scroll="end">${joined?'1 % →':'Última columna →'}</button>${joined?'<button class="btn" data-scene-scroll="top">↑ Cima</button><button class="btn" data-scene-scroll="base">↓ Base</button>':''}</div><div class="wealthSceneWrap${joined?' joinedSceneWrap':''}" tabindex="0" role="region" aria-label="Pilas monetarias de tamaño fijo; desplazamiento horizontal${joined?' y vertical':''}">${svg}</div><div class="sceneActions"><p class="caption">${escape(note)}</p>${button}</div>`;
  }

  function sampleLabels(positions,samples,zero,unit,formatCompact) {
    const placed=[];
    return samples.filter(i=>i<positions.length).map(i=>{
      const {g,x,width,single}=positions[i];
      const text=(single?'1 % · ':'')+formatCompact(g.mean),anchor=i>=98?'end':'middle',cx=x+width/2;
      const slots=single?Math.ceil(Math.abs(g.mean)/unit):Math.min(ROWS,Math.ceil(Math.abs(g.mean)/unit));
      const labelWidth=text.length*6.5+8,left=anchor==='end'?cx-labelWidth:cx-labelWidth/2;
      let y=g.mean<0?zero-12:zero-slots*DY-12;
      while(placed.some(p=>left<p.right&&left+labelWidth>p.left&&Math.abs(y-p.y)<17))y-=18;
      placed.push({left,right:left+labelWidth,y});
      return `<text class="wealth-sample-label" data-group="${escape(g.group)}" x="${cx}" y="${y}" fill="#d2d9c5" font-size="12" text-anchor="${anchor}">${escape(text)}</text>`;
    }).join('');
  }

  function render({name,population,percentiles,quintiles,top,allPercentiles,silhouette,split=true,unit=50000,formatMoney,formatCompact,formatPercent,formatNumber}) {
    const positiveRows=split?ROWS:Math.max(ROWS,Math.ceil(Math.max(...allPercentiles.map(g=>g.mean))/(unit*ROWS))*ROWS);
    const T=166,zero=T+positiveRows*DY;
    const main=split?percentiles.slice(0,-1):percentiles;
    // Other countries' debts must not create a large empty area below this chart.
    // The selected monetary unit and the physical block size remain unchanged.
    const debtRows=Math.ceil(Math.ceil(Math.max(0,...percentiles.map(g=>-g.mean))/unit)/5)*5;
    const bottom=zero+debtRows*DY,bandY=bottom+23,axisY=bandY+39,panelY=axisY+44,height=panelY+112;
    let cursor=L;
    const positions=main.map(g=>{const single=!split&&g.b===100,count=single?1:Math.max(1,decompose(g.mean,unit).columns.length),width=count*DX+(count>1?5:1);const pos={g,x:cursor,width,single,count};cursor+=width;return pos;});
    const mainRight=cursor,sideX=mainRight+50;
    const topPack=decompose(top.mean,unit),sideWidth=Math.max(198,topPack.columns.length*DX+32);
    const width=Math.max(1440,split?sideX+sideWidth+32:mainRight+32);
    const hint=g=>`≈ ${formatNumber(g.count)} adultos · ${g.a}–${g.b} % de la población adulta|Patrimonio medio por adulto: ${formatMoney(g.mean)}|${packing(g.mean,unit,formatNumber,formatMoney,!split&&g.b===100)}|Patrimonio total del grupo: ${formatCompact(g.total)}|Cuota del patrimonio nacional: ${formatPercent(g.share)}|Entrada desde ${formatMoney(g.lower)}|${name} · 2024 · Recuento estimado WID`;
    let body=positions.map(({g,x,single})=>pile(g,x,zero,unit,hint(g),single,single)).join('');
    body+=positions.filter(p=>p.count>1).map(({g,x,count})=>`<path class="wealth-overflow-guide" data-group="${escape(g.group)}" d="M${x} ${T-2}v-5h${count*DX}v5" fill="none" stroke="#aab19f" stroke-width=".8"/>`).join('');
    const samples=split?[9,29,49,69,84,94,98]:[9,29,49,69,84,94,99];
    body+=sampleLabels(positions,samples,zero,unit,formatCompact);
    body+=quintiles.map((g,i)=>{
      const members=positions.filter(p=>p.g.a>=g.a&&p.g.b<=g.b),x=members[0].x,end=members.at(-1).x+members.at(-1).width,w=end-x,cx=x+w/2,c=palette[i];
      return `<g class="wealth-quintile" data-count="${g.count}"><rect x="${x}" y="${bandY}" width="${w-2}" height="12" fill="${c}" opacity=".8"/><rect x="${x}" y="${panelY}" width="${w-3}" height="100" fill="${c}" opacity=".04"/><line x1="${x}" x2="${end-3}" y1="${panelY}" y2="${panelY}" stroke="${c}" stroke-opacity=".75"/><text x="${cx}" y="${panelY+22}" text-anchor="middle" fill="${c}" font-size="12">Q${i+1} · ${g.a}–${g.b} %</text><text class="wealth-quintile-count" x="${cx}" y="${panelY+46}" text-anchor="middle" fill="#e4e8dc" font-size="17">≈ ${escape(formatNumber(g.count))}</text><text x="${cx}" y="${panelY+63}" text-anchor="middle" fill="#9fa894" font-size="10">adultos estimados</text><text x="${cx}" y="${panelY+89}" text-anchor="middle" fill="${c}" font-size="22" font-weight="600">${escape(formatPercent(g.share))}</text></g>`;
    }).join('');
    for(let i=0;i<=main.length;i+=10){const x=i===main.length?mainRight:positions[i].x;body+=`<text class="wealth-population-tick" data-count="${population*i/100}" x="${x}" y="${axisY}" fill="#b0b4a9" font-size="12" text-anchor="middle">${escape(formatNumber(population*i/100))}</text>`;}
    if(split)body+=`<text class="wealth-population-tick" data-count="${population*.99}" x="${mainRight}" y="${axisY}" fill="#b0b4a9" font-size="12" text-anchor="middle">${escape(formatNumber(population*.99))}</text>`;
    body+=`<text x="${(L+mainRight)/2}" y="${axisY+26}" fill="#aab19f" font-size="10" text-anchor="middle">Adultos acumulados por intervalos · Las columnas adicionales representan dinero, no más población</text>`;
    if(split){
      const cx=sideX+sideWidth/2;
      body+=`<line x1="${sideX-24}" x2="${sideX-24}" y1="30" y2="${height-12}" stroke="#555c4e" stroke-dasharray="4 6"/><text x="${cx}" y="35" text-anchor="middle" fill="#cdd2c4" font-size="13">EL 1 % SUPERIOR</text><text class="wealth-top-count" x="${cx}" y="60" text-anchor="middle" fill="#e4e8dc" font-size="17">≈ ${escape(formatNumber(top.count))} adultos</text><text x="${cx}" y="93" text-anchor="middle" fill="#fef702" font-size="29" font-weight="700">${escape(formatCompact(top.mean))}</text><text x="${cx}" y="115" text-anchor="middle" fill="#b1b9a5" font-size="11">${topPack.fullColumns} completas${topPack.columns.length>topPack.fullColumns?' + última con resto':''}</text>`;
      body+=pile(top,cx-topPack.columns.length*DX/2,zero,unit,hint(top),true);
      body+=`<text x="${cx}" y="${panelY+44}" fill="#fef702" text-anchor="middle" font-size="25" font-weight="600">${escape(formatPercent(top.share))}</text><text x="${cx}" y="${panelY+67}" fill="#b0b6a7" text-anchor="middle" font-size="11">de la riqueza nacional</text>`;
    }
    const header=`<text x="${L}" y="55" fill="#e9ece3" font-size="37" font-weight="700" letter-spacing="1.6">${escape(name.toLocaleUpperCase('es'))}</text><text class="wealth-population-total" x="${L}" y="80" fill="#bdc5b2" font-size="13">${escape(formatNumber(population))} ADULTOS DE 20 AÑOS O MÁS · 2024</text><text x="${L}" y="112" fill="#b2b9a8" font-size="13">Cada intervalo agrupa ≈ ${escape(formatNumber(percentiles[0].count))} adultos, ordenados por patrimonio →</text>`;
    const svg=frame({width,height,name,population,unit,T,zero,debtRows,silhouette,formatCompact,header,body,positiveRows,attributes:`data-kind="national" data-split="${split}" data-top-layout="${split?'wrapped':'single'}" data-top-columns="${split?topPack.columns.length:1}" data-unfolded-top-columns="${topPack.columns.length}" data-top-value="${top.mean}"`,description:'Intervalos del 1 % de adultos, con su patrimonio medio. La población se indica en cifras, no por el número de columnas monetarias.'});
    const note=`Referencia fija: ${formatMoney(unit)} por bloque entero; ${formatMoney(unit*ROWS)} por columna lateral completa. ${split?'Se llenan las columnas de izquierda a derecha; no se igualan sus alturas.':'Todos juntos: las columnas laterales del 1 % se apilan en una sola barra vertical, sin el límite de 100 bloques y sin cambiar el valor ni el tamaño de los bloques. El eje vertical se amplía con la misma referencia para los once países.'} Los corchetes agrupan las columnas adicionales de un mismo intervalo: no son caídas de patrimonio ni más población. Una marca naranja indica el resto menor que un bloque, detallado al pasar el cursor o enfocar la pila. No se modifican ni redondean los importes del JSON a la unidad gráfica. Los bloques rojos son deuda neta. ${split?'Q5 incluye el 1 % separado.':'Cada quintil incluye todos sus intervalos.'}`;
    return {html:wrap(svg,unit,formatMoney,note,'<button class="btn" data-view="cima">Ampliar el 1 % →</button>',!split,formatCompact(top.mean))};
  }

  function renderTop({name,population,base,intervals,silhouette,unit=50000,formatMoney,formatCompact,formatPercent,formatNumber,formatGroup}) {
    const T=188,zero=T+ROWS*DY,bandY=zero+23,panelY=bandY+42,height=panelY+142;
    const parts=intervals.filter(g=>g.a>=base.a),top=parts.at(-1),focusLabel=fraction(base.fraction),topLabel=fraction(top.fraction);
    const widths=parts.map(g=>Math.max(248,decompose(g.mean,unit).columns.length*DX+32));
    const spare=Math.max(0,1440-L-32-widths.reduce((a,b)=>a+b,0));
    let cursor=L;
    const positions=parts.map((g,i)=>{const w=widths[i]+spare/parts.length,p={g,x:cursor,width:w};cursor+=w;return p;});
    const width=cursor+32;
    const hint=g=>`${formatGroup(g.group)} · ≈ ${formatNumber(g.count)} adultos|Patrimonio medio por adulto: ${formatMoney(g.mean)}|${packing(g.mean,unit,formatNumber,formatMoney)}|Patrimonio total del tramo: ${formatCompact(g.total)}|${formatPercent(g.share)} de la riqueza nacional|${formatPercent(g.share/base.share)} de la riqueza del ${focusLabel} superior|Entrada desde ${formatMoney(g.lower)}${g.upper==null?' · Sin límite superior':' · Hasta '+formatMoney(g.upper)}|Intervalo sin interpolación individual · WID 2024`;
    let body='';
    positions.forEach(({g,x,width:w},i)=>{
      const p=decompose(g.mean,unit),c=palette[intervals.indexOf(g)+1],tx=x+16;
      body+=`<line x1="${x-1}" x2="${x-1}" y1="138" y2="${height-12}" stroke="#555c4e" stroke-dasharray="4 6"/><text x="${tx}" y="151" fill="${c}" font-size="13">${i+1} · ${escape(formatGroup(g.group))}</text><text x="${tx}" y="176" fill="#e9ece3" font-size="20" font-weight="600">${escape(formatCompact(g.mean))} por adulto</text>`;
      body+=pile(g,tx,zero,unit,hint(g),i===parts.length-1);
      body+=`<rect x="${x}" y="${bandY}" width="${w-3}" height="8" fill="${c}" opacity=".8"/><g class="wealth-tail-summary" data-group="${escape(g.group)}" data-count="${g.count}"><rect x="${x}" y="${panelY}" width="${w-3}" height="130" fill="${c}" opacity=".04"/><text x="${tx}" y="${panelY+22}" fill="#e4e8dc" font-size="16">≈ ${escape(formatNumber(g.count))} adultos</text><text x="${tx}" y="${panelY+49}" fill="${c}" font-size="22" font-weight="600">${escape(formatPercent(g.share))}</text><text x="${tx}" y="${panelY+65}" fill="#aab19f" font-size="10">de la riqueza nacional</text><text x="${tx}" y="${panelY+89}" fill="#d1d8c7" font-size="12">${escape(formatPercent(g.share/base.share))} de la riqueza del ${escape(focusLabel)} superior</text><text x="${tx}" y="${panelY+113}" fill="#aab19f" font-size="11">${p.fullColumns} columnas completas${p.columns.length>p.fullColumns?' + última con '+escape(formatCompact(p.columns.at(-1).value)):''}</text></g>`;
    });
    const header=`<text x="${L}" y="55" fill="#e9ece3" font-size="37" font-weight="700" letter-spacing="1.6">${escape(name.toLocaleUpperCase('es'))}</text><text class="wealth-population-total" x="${L}" y="80" fill="#bdc5b2" font-size="13">DENTRO DEL ${escape(focusLabel)} SUPERIOR · ≈ ${escape(formatNumber(base.count))} ADULTOS · 2024</text><text x="${L}" y="109" fill="#d2d9c5" font-size="13">Media del grupo: ${escape(formatCompact(base.mean))} · Entrada desde ${escape(formatCompact(base.lower))} · ${escape(formatPercent(base.share))} de la riqueza nacional</text>`;
    const topPack=decompose(top.mean,unit);
    const svg=frame({width,height,name,population,unit,T,zero,debtRows:0,silhouette,formatCompact,header,body,attributes:`data-kind="top" data-focus-population="${base.count}" data-focus-label="${escape(focusLabel)}" data-focus-group="${escape(base.group)}" data-top-label="${escape(topLabel)}" data-top-columns="${topPack.columns.length}" data-top-value="${top.mean}" data-split="true"`,description:'Tramos exclusivos del grupo más rico. Cada pila suma el patrimonio medio por adulto de su tramo; el ancho monetario no representa población.'});
    const note=`La unidad de ${formatMoney(unit)} es idéntica a la del reparto general y se mantiene al cambiar de país. Todas las columnas completas contienen 100 bloques (${formatMoney(unit*ROWS)}); solo la última puede quedar incompleta. La marca naranja no es un bloque entero: identifica el resto menor que ${formatMoney(unit)}. Los anchos expresan columnas monetarias, no población; los adultos de cada tramo se indican aparte. No se interpolan datos ni se inventa una distribución dentro de cada tramo.`;
    return {html:wrap(svg,unit,formatMoney,note,'<button class="btn" data-view="quintiles">← Volver al reparto</button>')};
  }
  return {render,renderTop,decompose};
})();
