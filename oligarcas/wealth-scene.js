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
    if(single)return `Una sola barra vertical: ${formatNumber(p.blocks)} bloques enteros de ${formatMoney(unit)}${p.remainder?' + resto '+precise(p.remainder):''}. Sin límite de 100 bloques ni desbordamiento lateral${p.negative?' (deuda neta)':''}`;
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
    return `<defs><pattern id="wealth-cash" patternUnits="userSpaceOnUse" width="${DX}" height="${DY}"><rect x="1" y=".5" width="8" height="${DY-1}" rx=".25" fill="#7fab63"/><path d="M2 1.5H8" stroke="#bdd4a5" stroke-width=".45" opacity=".6"/></pattern><pattern id="wealth-richest-cash" patternUnits="userSpaceOnUse" width="${DX}" height="${DY}"><rect x="1" y=".5" width="8" height="${DY-1}" rx=".25" fill="#e7c766"/><path d="M2 1.5H8" stroke="#fff3ba" stroke-width=".45" opacity=".7"/></pattern><pattern id="wealth-debt" patternUnits="userSpaceOnUse" width="${DX}" height="${DY}"><rect x="1" y=".5" width="8" height="${DY-1}" fill="#dc8c82"/></pattern><pattern id="wealth-rest" patternUnits="userSpaceOnUse" width="3" height="3"><path d="M-1 1L1-1M0 3L3 0M2 4L4 2" stroke="#f3b865" stroke-width=".7"/></pattern><style>.wealth-pile:focus{outline:none}.wealth-pile:focus .wealth-hit,.wealth-pile:hover .wealth-hit{stroke:#fef702;stroke-width:.8}.wealth-column,.wealth-top-column{shape-rendering:crispEdges}</style></defs>`;
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
  function frame({width,height,name,population,unit,T,zero,debtRows,silhouette,formatCompact,attributes,header,body,description,positiveRows=ROWS,singleBars=false}) {
    return `<svg id="wealthGraphic" class="moneyScene fixedBlockScene" xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" style="--scene-width:${width}px" data-population="${population}" data-block-unit="${unit}" data-stack-capacity="${unit*ROWS}" data-block-rows="${ROWS}" data-plot-rows="${positiveRows}" data-block-width="8" data-block-height="${DY-1}" data-row-step="${DY}" data-upper="${unit*positiveRows}" data-lower="${-debtRows*unit}" ${attributes} role="group" aria-label="${escape(name)}: patrimonio en bloques monetarios de valor fijo" font-family="Arial, sans-serif"><title>${escape(name)}: bloques de ${escape(precise(unit))}</title><desc>${escape(description)} Todos los países y vistas comparten la misma unidad fija. La fortuna Forbes se muestra entera y proporcional en la escala lineal superior; su columna de bloques en el detalle continúa fuera de la ventana. La cifra original se publica en USD de 2026 y la conversión a EUR es nominal, no EUR de 2025. ${singleBars?'Cada intervalo del eje principal forma una sola barra vertical, sin límite de altura ni columnas adicionales a su derecha. Solo el 1 % separado conserva el apilado lateral.':'Las columnas laterales completas contienen 100 bloques.'} Las marcas naranjas son restos menores que un bloque, no unidades completas.</desc>${defs()}<rect width="${width}" height="${height}" fill="#252825"/>${map(silhouette,zero-ROWS*DY,width)}${grid(width,zero,unit,debtRows,formatCompact,positiveRows)}${header}${body}</svg>`;
  }
  function reference(unit,formatMoney,singleBars=false) {
    return `<div class="blockReference"><strong><i class="cashKey"></i>1 bloque entero = ${escape(formatMoney(unit))}</strong><span>100 bloques = ${escape(formatMoney(unit*ROWS))}</span><span class="blockRemainderKey"><i></i>Marca naranja = resto menor que un bloque, importe en el detalle</span><span><i class="richestKey"></i>Dorado = fortuna Forbes 2026, altura lineal completa en la gráfica superior</span><small>EUR de 2025 por adulto en WID · Misma unidad en todos los países y vistas · Fortuna Forbes 2026: USD original y EUR nominal convertido</small></div>`;
  }
  function wrap(svg,unit,formatMoney,note,button,{singleBars=false,scrollable=false,split=false,richest=false,overview=''}={}) {
    const hint=singleBars?`Cada intervalo crece en una sola barra vertical, sin cortes ni sobrantes a la derecha.${split?' Solo el 1 % separado se apila lateralmente.':''}${scrollable?' Desplázate ↕ para ver las barras completas.':''}`:'Desplaza la gráfica ↔: el dinero que supera una columna continúa en la siguiente. Las pilas no se encogen para caber.';
    return reference(unit,formatMoney,singleBars)+overview+`<div class="fixedSceneNavigation"><p class="fixedSceneHint">${hint}</p><button class="btn" data-scene-scroll="start">← Inicio</button><button class="btn" data-scene-scroll="end">${singleBars?(split?'1 % separado →':'1 % →'):'Última columna →'}</button>${richest?'<button class="btn" data-scene-scroll="richest">Persona más rica →</button>':''}${scrollable?'<button class="btn" data-scene-scroll="top">↑ Cima</button><button class="btn" data-scene-scroll="base">↓ Base</button>':''}</div><div class="wealthSceneWrap${scrollable?' verticalSceneWrap':''}${singleBars&&!split?' joinedSceneWrap':''}" tabindex="0" role="region" aria-label="Pilas monetarias de tamaño fijo; desplazamiento horizontal${scrollable?' y vertical':''}">${svg}</div><div class="sceneActions"><p class="caption">${escape(note)}</p>${button}</div>`;
  }

  // The detailed plot is a viewport at the fixed 10,000-euro block scale.
  // One patterned SVG rectangle represents every block without millions of nodes.
  function richestPanel({person,source,x,T,zero,panelY,height,unit,formatMoney,formatCompact,formatNumber}) {
    if(!person)return '';
    const amount=person.usd_billions*1e9/source.usd_per_eur;
    const totalBlocks=amount/unit,realHeight=totalBlocks*DY;
    const top=Math.max(T,zero-realHeight),cx=x+135,usd=formatNumber(person.usd_billions*1000)+' M$';
    const details=`${person.name} · Forbes, 1-3-2026|USD originales: ${formatNumber(person.usd_billions*1000)} M$|Conversión a EUR nominales: ${formatCompact(amount)}; 1 € = ${source.usd_per_eur} $ (BCE, 27-2-2026)|≈ ${formatNumber(totalBlocks)} bloques de ${formatMoney(unit)}|La columna continúa fuera de la ventana de detalle; la escala completa muestra la altura total|Según ciudadanía Forbes, no residencia. No es WID 2024 ni EUR de 2025${person.includes_family?'|Forbes atribuye la cifra al hombre y su familia':''}`;
    return `<g id="wealthRichestPanel" class="wealth-richest" data-name="${escape(person.name)}" data-usd-billions="${person.usd_billions}" data-eur-nominal="${amount}" data-total-blocks="${totalBlocks}" data-true-height="${realHeight}" data-viewport-height="${zero-T}" data-source-year="2026" data-x="${x}" tabindex="0" role="img" aria-label="${escape(details.replaceAll('|','. '))}" data-tip="${escape(details)}"><line x1="${x-18}" x2="${x-18}" y1="30" y2="${height-12}" stroke="#8c7542" stroke-dasharray="4 6"/><text x="${cx}" y="35" text-anchor="middle" fill="#efd696" font-size="12">${person.includes_family?'HOMBRE MÁS RICO Y FAMILIA':'HOMBRE MÁS RICO'}</text><text x="${cx}" y="60" text-anchor="middle" fill="#e8ede2" font-size="15" font-weight="600">${escape(person.name)}</text><text x="${cx}" y="91" text-anchor="middle" fill="#ffd766" font-size="23" font-weight="700">${usd}</text><text x="${cx}" y="110" text-anchor="middle" fill="#cfc9b6" font-size="11">≈ ${escape(formatCompact(amount))} · EUR nominal</text><text x="${cx}" y="134" text-anchor="middle" fill="#efd696" font-size="11">↑ continúa fuera del detalle</text><rect class="wealth-richest-column" x="${cx-5}" y="${top}" width="${DX}" height="${zero-top}" fill="url(#wealth-richest-cash)"/><rect class="wealth-richest-hit" x="${x}" y="${T}" width="270" height="${zero-T}" fill="transparent"/><text x="${cx}" y="${panelY+22}" text-anchor="middle" fill="#e8ede2" font-size="13">${escape(person.name)}</text><text x="${cx}" y="${panelY+44}" text-anchor="middle" fill="#ffd766" font-size="18" font-weight="700">${usd}</text><text x="${cx}" y="${panelY+64}" text-anchor="middle" fill="#efd696" font-size="12">≈ ${escape(formatCompact(amount))}</text><text x="${cx}" y="${panelY+82}" text-anchor="middle" fill="#bcb8a8" font-size="11">Ver altura total arriba · Forbes 2026</text></g>`;
  }

  // An unbroken linear overview: the millionaire bars really are subpixel
  // beside a billionaire fortune. The detailed scene below keeps the 10k unit.
  function renderOverview({person,source,groups,formatCompact,formatNumber,focus}) {
    if(!person)return '';
    const amount=person.usd_billions*1e9/source.usd_per_eur;
    const max=Math.max(amount,...groups.map(g=>Math.max(0,g.mean)));
    const scale=265/max,base=380,start=70,end=1090,slot=(end-start)/groups.length;
    const bars=groups.map((g,i)=>{
      const h=g.mean*scale,x=start+i*slot,w=Math.max(2,Math.min(8,slot-1));
      const y=h>=0?base-h:base,shade=focus==='national'?palette[Math.min(4,Math.floor(g.a/20))]:palette[Math.min(4,i+1)];
      const tip=`${g.group} · WID 2024|Media por adulto: ${formatCompact(g.mean)}|Altura lineal en esta escala: ${new Intl.NumberFormat('es-ES',{maximumSignificantDigits:3}).format(Math.abs(h))} píxeles`;
      return `<rect class="wealth-overview-bar" x="${x}" y="${y}" width="${w}" height="${Math.abs(h)}" fill="${shade}" data-value="${g.mean}" data-height="${Math.abs(h)}" data-tip="${escape(tip)}"><title>${escape(g.group)}: ${escape(formatCompact(g.mean))}</title></rect>`;
    }).join('');
    const subpixel=groups.filter(g=>Math.abs(g.mean)*scale<1).length;
    const fx=person.usd_billions*1000,personTip=`${person.name} · Forbes 2026|${formatNumber(fx)} M$ originales|≈ ${formatCompact(amount)} nominales; cambio BCE ${source.usd_per_eur} USD/EUR|Misma escala lineal que las medias WID; fechas y bases de precios distintas`;
    return `<section class="wealthOverview" aria-label="Comparación lineal completa"><div class="wealthOverviewHead"><strong>Escala completa · todos en la misma gráfica</strong><span>Alturas proporcionales al patrimonio, sin cortes ni mínimos visuales</span></div><div class="wealthOverviewCanvas"><svg id="wealthOverviewGraphic" xmlns="http://www.w3.org/2000/svg" width="1300" height="440" viewBox="0 0 1300 440" role="img" aria-label="Escala lineal completa para ${escape(focus==='national'?'los percentiles y la persona más rica':'los tramos del 1 % y la persona más rica')}" data-max="${max}" data-eur-nominal="${amount}" data-scale="${scale}" data-subpixel-bars="${subpixel}" font-family="Arial, sans-serif"><rect width="1300" height="440" fill="#252825"/><line x1="50" x2="1260" y1="${base}" y2="${base}" stroke="#7b8177"/><line x1="50" x2="1260" y1="115" y2="115" stroke="#43483f"/><text x="48" y="${base+4}" fill="#b0b7a6" text-anchor="end" font-size="12">0 €</text><text x="75" y="107" fill="#b0b7a6" text-anchor="start" font-size="12">${escape(formatCompact(max))}</text>${bars}<line x1="1135" x2="1135" y1="98" y2="416" stroke="#8c7542" stroke-dasharray="4 6"/><rect class="wealth-overview-richest" x="1190" y="${base-amount*scale}" width="30" height="${amount*scale}" fill="#e7c766" data-value="${amount}" data-height="${amount*scale}" data-tip="${escape(personTip)}"><title>${escape(person.name)}: ${formatNumber(fx)} M$</title></rect><text x="1205" y="49" text-anchor="middle" fill="#ffd766" font-size="16" font-weight="700">${escape(formatNumber(fx))} M$</text><text x="1205" y="72" text-anchor="middle" fill="#e9ece3" font-size="12">≈ ${escape(formatCompact(amount))}</text><text x="1205" y="405" text-anchor="middle" fill="#e9ece3" font-size="12">${escape(person.name)}</text><text x="${(start+end)/2}" y="405" text-anchor="middle" fill="#b9c7ad" font-size="12">${focus==='national'?'Intervalos de la población (WID)':'Tramos dentro del 1 % (WID)'}</text></svg></div><p class="caption">${subpixel} de ${groups.length} barras WID miden menos de 1 píxel a esta escala: su pequeñez es la diferencia real, no un redondeo. Forbes: ${escape(formatNumber(fx))} M$ originales (1-3-2026), equivalentes a ≈ ${escape(formatCompact(amount))} nominales según BCE. WID: EUR de 2025 y distribución de 2024. Desplázate a la gráfica inferior para ver sus bloques de 10.000 €.</p></section>`;
  }

  function sampleLabels(positions,samples,zero,unit,formatCompact) {
    const placed=[];
    return samples.filter(i=>i<positions.length).map(i=>{
      const {g,x,width,single}=positions[i];
      const text=(g.b===100?'1 % · ':'')+formatCompact(g.mean),anchor=i>=98?'end':'middle',cx=x+width/2;
      const slots=single?Math.ceil(Math.abs(g.mean)/unit):Math.min(ROWS,Math.ceil(Math.abs(g.mean)/unit));
      const labelWidth=text.length*6.5+8,left=anchor==='end'?cx-labelWidth:cx-labelWidth/2;
      let y=g.mean<0?zero-12:zero-slots*DY-12;
      while(placed.some(p=>left<p.right&&left+labelWidth>p.left&&Math.abs(y-p.y)<17))y-=18;
      placed.push({left,right:left+labelWidth,y});
      return `<text class="wealth-sample-label" data-group="${escape(g.group)}" x="${cx}" y="${y}" fill="#d2d9c5" font-size="12" text-anchor="${anchor}">${escape(text)}</text>`;
    }).join('');
  }

  function render({name,population,percentiles,quintiles,top,allPercentiles,silhouette,split=true,unit=10000,richestMan,richestSource,formatMoney,formatCompact,formatPercent,formatNumber}) {
    // All principal intervals grow upwards. Only the intentionally separated top
    // 1% wraps sideways. A common ceiling preserves cross-country comparison.
    const referencePercentiles=allPercentiles.filter(g=>!split||g.b<100);
    const positiveRows=Math.max(ROWS,Math.ceil(Math.max(...referencePercentiles.map(g=>g.mean))/(unit*ROWS))*ROWS);
    const T=166,zero=T+positiveRows*DY;
    const main=split?percentiles.slice(0,-1):percentiles;
    // Other countries' debts must not create a large empty area below this chart.
    // The selected monetary unit and the physical block size remain unchanged.
    const debtRows=Math.ceil(Math.ceil(Math.max(0,...percentiles.map(g=>-g.mean))/unit)/5)*5;
    const bottom=zero+debtRows*DY,bandY=bottom+23,axisY=bandY+39,panelY=axisY+44,height=panelY+112;
    let cursor=L;
    const positions=main.map(g=>{const width=DX+1,pos={g,x:cursor,width,single:true};cursor+=width;return pos;});
    const mainRight=cursor,sideX=mainRight+50;
    const topPack=decompose(top.mean,unit),sideWidth=Math.max(198,topPack.columns.length*DX+32);
    const richestX=(split?sideX+sideWidth:mainRight)+72;
    const width=Math.max(1440,richestMan?richestX+270+32:split?sideX+sideWidth+32:mainRight+32);
    const hint=(g,single=true)=>`≈ ${formatNumber(g.count)} adultos · ${g.a}–${g.b} % de la población adulta|Patrimonio medio por adulto: ${formatMoney(g.mean)}|${packing(g.mean,unit,formatNumber,formatMoney,single)}|Patrimonio total del grupo: ${formatCompact(g.total)}|Cuota del patrimonio nacional: ${formatPercent(g.share)}|Entrada desde ${formatMoney(g.lower)}|${name} · 2024 · Recuento estimado WID`;
    let body=positions.map(({g,x})=>pile(g,x,zero,unit,hint(g),g.b===100,true)).join('');
    const samples=split?[9,29,49,69,84,94,98]:[9,29,49,69,84,94,99];
    body+=sampleLabels(positions,samples,zero,unit,formatCompact);
    body+=quintiles.map((g,i)=>{
      const members=positions.filter(p=>p.g.a>=g.a&&p.g.b<=g.b),x=members[0].x,end=members.at(-1).x+members.at(-1).width,w=end-x,cx=x+w/2,c=palette[i];
      return `<g class="wealth-quintile" data-count="${g.count}"><rect x="${x}" y="${bandY}" width="${w-2}" height="12" fill="${c}" opacity=".8"/><rect x="${x}" y="${panelY}" width="${w-3}" height="100" fill="${c}" opacity=".04"/><line x1="${x}" x2="${end-3}" y1="${panelY}" y2="${panelY}" stroke="${c}" stroke-opacity=".75"/><text x="${cx}" y="${panelY+22}" text-anchor="middle" fill="${c}" font-size="12">Q${i+1} · ${g.a}–${g.b} %</text><text class="wealth-quintile-count" x="${cx}" y="${panelY+46}" text-anchor="middle" fill="#e4e8dc" font-size="17">≈ ${escape(formatNumber(g.count))}</text><text x="${cx}" y="${panelY+63}" text-anchor="middle" fill="#9fa894" font-size="10">adultos estimados</text><text x="${cx}" y="${panelY+89}" text-anchor="middle" fill="${c}" font-size="22" font-weight="600">${escape(formatPercent(g.share))}</text></g>`;
    }).join('');
    for(let i=0;i<=main.length;i+=10){const x=i===main.length?mainRight:positions[i].x;body+=`<text class="wealth-population-tick" data-count="${population*i/100}" x="${x}" y="${axisY}" fill="#b0b4a9" font-size="12" text-anchor="middle">${escape(formatNumber(population*i/100))}</text>`;}
    if(split)body+=`<text class="wealth-population-tick" data-count="${population*.99}" x="${mainRight}" y="${axisY}" fill="#b0b4a9" font-size="12" text-anchor="middle">${escape(formatNumber(population*.99))}</text>`;
    body+=`<text x="${(L+mainRight)/2}" y="${axisY+26}" fill="#aab19f" font-size="10" text-anchor="middle">Adultos acumulados por intervalos · Una sola barra vertical por intervalo</text>`;
    if(split){
      const cx=sideX+sideWidth/2;
      body+=`<line x1="${sideX-24}" x2="${sideX-24}" y1="30" y2="${height-12}" stroke="#555c4e" stroke-dasharray="4 6"/><text x="${cx}" y="35" text-anchor="middle" fill="#cdd2c4" font-size="13">EL 1 % SUPERIOR</text><text class="wealth-top-count" x="${cx}" y="60" text-anchor="middle" fill="#e4e8dc" font-size="17">≈ ${escape(formatNumber(top.count))} adultos</text><text x="${cx}" y="93" text-anchor="middle" fill="#fef702" font-size="29" font-weight="700">${escape(formatCompact(top.mean))}</text><text x="${cx}" y="115" text-anchor="middle" fill="#b1b9a5" font-size="11">${topPack.fullColumns} completas${topPack.columns.length>topPack.fullColumns?' + última con resto':''}</text>`;
      body+=pile(top,cx-topPack.columns.length*DX/2,zero,unit,hint(top,false),true);
      body+=`<text x="${cx}" y="${panelY+44}" fill="#fef702" text-anchor="middle" font-size="25" font-weight="600">${escape(formatPercent(top.share))}</text><text x="${cx}" y="${panelY+67}" fill="#b0b6a7" text-anchor="middle" font-size="11">de la riqueza nacional</text>`;
    }
    body+=richestPanel({person:richestMan,source:richestSource,x:richestX,T,zero,panelY,height,unit,formatMoney,formatCompact,formatNumber});
    const header=`<text x="${L}" y="55" fill="#e9ece3" font-size="37" font-weight="700" letter-spacing="1.6">${escape(name.toLocaleUpperCase('es'))}</text><text class="wealth-population-total" x="${L}" y="80" fill="#bdc5b2" font-size="13">${escape(formatNumber(population))} ADULTOS DE 20 AÑOS O MÁS · 2024</text><text x="${L}" y="112" fill="#b2b9a8" font-size="13">Cada intervalo agrupa ≈ ${escape(formatNumber(percentiles[0].count))} adultos, ordenados por patrimonio →</text>`;
    const svg=frame({width,height,name,population,unit,T,zero,debtRows,silhouette,formatCompact,header,body,positiveRows,singleBars:true,attributes:`data-kind="national" data-main-layout="single" data-split="${split}" data-top-layout="${split?'wrapped':'single'}" data-top-columns="${split?topPack.columns.length:1}" data-unfolded-top-columns="${topPack.columns.length}" data-top-value="${top.mean}" data-scene-main-end="${split?sideX:mainRight}" ${richestMan?'data-richest-x="'+richestX+'"':''}`,description:'Intervalos del 1 % de adultos, con su patrimonio medio. La población se indica en cifras, no por el número de columnas monetarias.'});
    const note=`Referencia fija: ${formatMoney(unit)} por bloque entero. Cada intervalo del eje principal forma una sola barra vertical, sin límite de 100 bloques. ${split?'Únicamente el 1 % separado se reparte en columnas laterales de 100 bloques; Q5 lo incluye.':'Todos juntos incluye el 1 % en una sola barra, igual que los demás intervalos.'} La columna dorada continúa por encima de esta ventana de detalle; la gráfica superior muestra su altura entera en escala lineal junto con las demás barras. Forbes publica la fortuna en USD de 2026; la conversión a EUR nominal no comparte fecha ni base de precios con WID.`;
    const overview=renderOverview({person:richestMan,source:richestSource,groups:split?[...main,top]:main,formatCompact,formatNumber,focus:'national'});
    return {html:wrap(svg,unit,formatMoney,note,'<button class="btn" data-view="cima">Ampliar el 1 % →</button>',{singleBars:true,scrollable:positiveRows>ROWS,split,richest:!!richestMan,overview})};
  }

  function renderTop({name,population,base,intervals,silhouette,unit=10000,richestMan,richestSource,formatMoney,formatCompact,formatPercent,formatNumber,formatGroup}) {
    const T=188,zero=T+ROWS*DY,bandY=zero+23,panelY=bandY+42,height=panelY+142;
    const parts=intervals.filter(g=>g.a>=base.a),top=parts.at(-1),focusLabel=fraction(base.fraction),topLabel=fraction(top.fraction);
    const widths=parts.map(g=>Math.max(248,decompose(g.mean,unit).columns.length*DX+32));
    const spare=Math.max(0,1440-L-32-widths.reduce((a,b)=>a+b,0));
    let cursor=L;
    const positions=parts.map((g,i)=>{const w=widths[i]+spare/parts.length,p={g,x:cursor,width:w};cursor+=w;return p;});
    const richestX=cursor+72,width=richestMan?richestX+270+32:cursor+32;
    const hint=g=>`${formatGroup(g.group)} · ≈ ${formatNumber(g.count)} adultos|Patrimonio medio por adulto: ${formatMoney(g.mean)}|${packing(g.mean,unit,formatNumber,formatMoney)}|Patrimonio total del tramo: ${formatCompact(g.total)}|${formatPercent(g.share)} de la riqueza nacional|${formatPercent(g.share/base.share)} de la riqueza del ${focusLabel} superior|Entrada desde ${formatMoney(g.lower)}${g.upper==null?' · Sin límite superior':' · Hasta '+formatMoney(g.upper)}|Intervalo sin interpolación individual · WID 2024`;
    let body='';
    positions.forEach(({g,x,width:w},i)=>{
      const p=decompose(g.mean,unit),c=palette[intervals.indexOf(g)+1],tx=x+16;
      body+=`<line x1="${x-1}" x2="${x-1}" y1="138" y2="${height-12}" stroke="#555c4e" stroke-dasharray="4 6"/><text x="${tx}" y="151" fill="${c}" font-size="13">${i+1} · ${escape(formatGroup(g.group))}</text><text x="${tx}" y="176" fill="#e9ece3" font-size="20" font-weight="600">${escape(formatCompact(g.mean))} por adulto</text>`;
      body+=pile(g,tx,zero,unit,hint(g),i===parts.length-1);
      body+=`<rect x="${x}" y="${bandY}" width="${w-3}" height="8" fill="${c}" opacity=".8"/><g class="wealth-tail-summary" data-group="${escape(g.group)}" data-count="${g.count}"><rect x="${x}" y="${panelY}" width="${w-3}" height="130" fill="${c}" opacity=".04"/><text x="${tx}" y="${panelY+22}" fill="#e4e8dc" font-size="16">≈ ${escape(formatNumber(g.count))} adultos</text><text x="${tx}" y="${panelY+49}" fill="${c}" font-size="22" font-weight="600">${escape(formatPercent(g.share))}</text><text x="${tx}" y="${panelY+65}" fill="#aab19f" font-size="10">de la riqueza nacional</text><text x="${tx}" y="${panelY+89}" fill="#d1d8c7" font-size="12">${escape(formatPercent(g.share/base.share))} de la riqueza del ${escape(focusLabel)} superior</text><text x="${tx}" y="${panelY+113}" fill="#aab19f" font-size="11">${p.fullColumns} columnas completas${p.columns.length>p.fullColumns?' + última con '+escape(formatCompact(p.columns.at(-1).value)):''}</text></g>`;
    });
    body+=richestPanel({person:richestMan,source:richestSource,x:richestX,T,zero,panelY,height,unit,formatMoney,formatCompact,formatNumber});
    const header=`<text x="${L}" y="55" fill="#e9ece3" font-size="37" font-weight="700" letter-spacing="1.6">${escape(name.toLocaleUpperCase('es'))}</text><text class="wealth-population-total" x="${L}" y="80" fill="#bdc5b2" font-size="13">DENTRO DEL ${escape(focusLabel)} SUPERIOR · ≈ ${escape(formatNumber(base.count))} ADULTOS · 2024</text><text x="${L}" y="109" fill="#d2d9c5" font-size="13">Media del grupo: ${escape(formatCompact(base.mean))} · Entrada desde ${escape(formatCompact(base.lower))} · ${escape(formatPercent(base.share))} de la riqueza nacional</text>`;
    const topPack=decompose(top.mean,unit);
    const svg=frame({width,height,name,population,unit,T,zero,debtRows:0,silhouette,formatCompact,header,body,attributes:`data-kind="top" data-focus-population="${base.count}" data-focus-label="${escape(focusLabel)}" data-focus-group="${escape(base.group)}" data-top-label="${escape(topLabel)}" data-top-columns="${topPack.columns.length}" data-top-value="${top.mean}" data-split="true" data-scene-main-end="${positions.at(-1).x}" ${richestMan?'data-richest-x="'+richestX+'"':''}`,description:'Tramos exclusivos del grupo más rico. Cada pila suma el patrimonio medio por adulto de su tramo; el ancho monetario no representa población.'});
    const note=`La unidad de ${formatMoney(unit)} es idéntica a la del reparto general. Cada columna lateral completa contiene 100 bloques; la dorada continúa fuera de la ventana de detalle a la misma escala. La gráfica superior muestra la fortuna entera y todos los tramos juntos, con alturas lineales proporcionales. Forbes publica USD de 2026; su conversión a EUR nominal no es un dato WID 2024 ni está ajustada a precios de 2025.`;
    const overview=renderOverview({person:richestMan,source:richestSource,groups:parts,formatCompact,formatNumber,focus:'top'});
    return {html:wrap(svg,unit,formatMoney,note,'<button class="btn" data-view="quintiles">← Volver al reparto</button>',{richest:!!richestMan,overview})};
  }
  return {render,renderTop,decompose};
})();
