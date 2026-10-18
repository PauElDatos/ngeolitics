'use strict';

// Every column groups 1% of the actual adult population at its published mean.
// The isolated top percentile is
// reflowed into equal columns, with exactly the same block dimensions and value.
window.WealthScene = (() => {
  const palette=['#67b6df','#69c7ae','#bdcd73','#eeb55e','#fef702'];
  const escape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const ceiling=v=>{if(v<=0)return 1;const power=10**Math.floor(Math.log10(v));return ([1,2,2.5,5,10].find(n=>n*power>=v)??10)*power;};
  const populationCompact=value=>value>=1e6?`${new Intl.NumberFormat('es-ES',{maximumFractionDigits:2}).format(value/1e6)} M`:new Intl.NumberFormat('es-ES',{maximumFractionDigits:0}).format(value);
  function render({name,country,population,percentiles,quintiles,top,allPercentiles,silhouette,split=true,common=false,formatMoney,formatCompact,formatPercent,formatNumber}) {
    const W=1440,H=846,L=92,topY=146,bottomY=628,mainRight=split?1186:1408;
    const main=split?percentiles.slice(0,99):percentiles;
    const reference=common?allPercentiles.filter(g=>!split||g.b<=99):main;
    const upper=ceiling(Math.max(...reference.map(g=>g.mean))*1.02);
    const lowest=Math.min(0,...reference.map(g=>g.mean));
    const lower=lowest<0?-Math.max(ceiling(-lowest),upper*.06):0;
    const plotHeight=bottomY-topY,range=upper-lower,y=v=>topY+(upper-v)/range*plotHeight,zero=y(0);
    const pitch=(mainRight-L)/main.length,unit=upper/100,blockHeight=unit/range*plotHeight;
    const stacks=split?Math.ceil(top.mean/upper):1,stackValue=top.mean/stacks;
    const sideCenter=1309,stackWidth=stacks*pitch;
    let grid='';
    for(let i=0;i<=5;i++){
      const value=upper*i/5,yy=y(value);
      grid+=`<line x1="${L}" x2="1408" y1="${yy}" y2="${yy}" stroke="${i===0?'#7b8177':'#43483f'}" stroke-width="${i===0?1.2:.8}"/><text x="${L-12}" y="${yy+4}" fill="#a1a59c" text-anchor="end" font-size="12">${escape(formatCompact(value))}</text>`;
    }
    if(lower<0&&-lower>=upper*.12){grid+=`<line x1="${L}" x2="1408" y1="${bottomY}" y2="${bottomY}" stroke="#704e4b" stroke-dasharray="3 5"/><text x="${L-12}" y="${bottomY+4}" fill="#dc9991" text-anchor="end" font-size="11">${escape(formatCompact(lower))}</text>`;}
    let guides='',ticks='';
    for(let i=0;i<=main.length;i+=10){const x=L+i*pitch;guides+=`<line x1="${x}" x2="${x}" y1="${topY}" y2="${bottomY}" stroke="#4a4e44" stroke-opacity=".36"/>`;ticks+=`<text class="wealth-population-tick" data-count="${population*i/100}" x="${x}" y="690" fill="#b0b4a9" font-size="14" text-anchor="middle">${escape(populationCompact(population*i/100))}</text>`;}
    if(split)ticks+=`<text class="wealth-population-tick" data-count="${population*.99}" x="${mainRight}" y="690" fill="#b0b4a9" font-size="14" text-anchor="middle">${escape(populationCompact(population*.99))}</text>`;
    ticks+=`<text x="${(L+mainRight)/2}" y="716" fill="#aab19f" font-size="11" text-anchor="middle">Adultos acumulados, de menor a mayor patrimonio · M = millones</text>`;
    const hint=g=>`≈ ${formatNumber(g.count)} adultos · ${g.a}–${g.b} % de la población adulta|Patrimonio medio por adulto: ${formatMoney(g.mean)}|Patrimonio total del grupo: ${formatCompact(g.total)}|Cuota del patrimonio nacional: ${formatPercent(g.share)}|Entrada desde ${formatMoney(g.lower)}|${name} · 2024 · Recuento estimado WID`;
    const columns=main.map((g,i)=>{
      const x=L+i*pitch,yy=y(g.mean),height=Math.abs(yy-zero),q=Math.min(4,Math.floor(g.a/20));
      return `<g class="wealth-interval" data-count="${g.count}" tabindex="0" role="img" aria-label="${escape(hint(g).replaceAll('|','. '))}" data-tip="${escape(hint(g))}"><rect class="wealth-column" data-group="${escape(g.group)}" data-value="${g.mean}" x="${x+.6}" y="${Math.min(yy,zero)}" width="${pitch-1.2}" height="${height}" fill="url(#${g.mean<0?'wealth-debt':'wealth-cash'})"/><rect class="wealth-hit" x="${x}" y="${topY}" width="${pitch}" height="520" fill="transparent"/><line class="wealth-focus" x1="${x+pitch/2}" x2="${x+pitch/2}" y1="${topY}" y2="641" stroke="${palette[q]}" stroke-opacity=".8" stroke-width="1" visibility="hidden"/></g>`;
    }).join('');
    const sampleIndices=split?[9,29,49,69,84,94,98]:[9,29,49,69,84,94,99];
    let amountLabels=sampleIndices.filter(i=>i<main.length).map(i=>{const g=main[i],x=L+(i+.5)*pitch,yy=Math.max(topY+17,y(Math.max(0,g.mean))-12);return `<text x="${x}" y="${yy}" fill="#d2d9c5" font-size="12" text-anchor="${i>=98?'end':'middle'}">${escape(formatCompact(g.mean))}</text>`;}).join('');
    const map=silhouette?`<svg x="${L+100}" y="${topY+30}" width="${mainRight-L-150}" height="${plotHeight-35}" viewBox="0 0 1000 500" preserveAspectRatio="xMidYMid meet" aria-hidden="true"><path d="${escape(silhouette.d)}" fill="#b1b8a2" opacity=".085"/></svg>`:'';
    const bands=quintiles.map((g,i)=>{
      const x=L+i*20*pitch,w=(i===4&&split?19:20)*pitch,cx=x+w/2;
      return `<g class="wealth-quintile" data-count="${g.count}" aria-label="Q${i+1}: aproximadamente ${escape(formatNumber(g.count))} adultos; ${escape(formatPercent(g.share))} de la riqueza"><rect class="wealth-population-band" x="${x}" y="651" width="${w-2}" height="12" fill="${palette[i]}" opacity=".8"/><rect x="${x}" y="734" width="${w-3}" height="100" fill="${palette[i]}" opacity=".04"/><line x1="${x}" x2="${x+w-3}" y1="734" y2="734" stroke="${palette[i]}" stroke-opacity=".75"/><text x="${cx}" y="756" text-anchor="middle" fill="${palette[i]}" font-size="12">Q${i+1} · ${g.a}–${g.b} %</text><text class="wealth-quintile-count" x="${cx}" y="780" text-anchor="middle" fill="#e4e8dc" font-size="17">≈ ${escape(formatNumber(g.count))}</text><text x="${cx}" y="797" text-anchor="middle" fill="#9fa894" font-size="10">adultos estimados</text><text x="${cx}" y="823" text-anchor="middle" fill="${palette[i]}" font-size="22" font-weight="600">${escape(formatPercent(g.share))}</text></g>`;
    }).join('');
    const reflow=split?`
      <line x1="1210" x2="1210" y1="30" y2="834" stroke="#555c4e" stroke-dasharray="4 6"/>
      <text x="${sideCenter}" y="35" text-anchor="middle" fill="#cdd2c4" font-size="13" letter-spacing="1.1">EL 1 % SUPERIOR</text>
      <text class="wealth-top-count" x="${sideCenter}" y="60" text-anchor="middle" fill="#e4e8dc" font-size="17">≈ ${escape(formatNumber(top.count))} adultos</text>
      <text x="${sideCenter}" y="93" text-anchor="middle" fill="#fef702" font-size="29" font-weight="700">${escape(formatCompact(top.mean))}</text>
      <text x="${sideCenter}" y="115" text-anchor="middle" fill="#b1b9a5" font-size="12">de patrimonio medio por adulto</text>
      <text x="${sideCenter}" y="134" fill="#b0b6a7" text-anchor="middle" font-size="10">${stacks} columnas reapiladas</text>
      ${Array.from({length:stacks},(_,i)=>`<g transform="translate(${sideCenter-stackWidth/2+i*pitch},0)"><rect class="wealth-top-column" x=".6" y="${y(stackValue)}" width="${pitch-1.2}" height="${zero-y(stackValue)}" fill="url(#wealth-top-cash)"/></g>`).join('')}
      <g class="wealth-top" data-count="${top.count}" tabindex="0" role="img" data-tip="${escape(hint(top)+'|Reapilado en '+stacks+' columnas: la suma conserva la media original.')}" aria-label="${escape('1 % superior: aproximadamente '+formatNumber(top.count)+' adultos. Media '+formatMoney(top.mean)+'. Cuota '+formatPercent(top.share))}"><rect x="${sideCenter-stackWidth/2-8}" y="${topY}" width="${stackWidth+16}" height="520" fill="transparent"/></g>
      <rect class="wealth-population-band" x="${sideCenter-stackWidth/2}" y="651" width="${stackWidth}" height="12" fill="${palette[4]}" opacity=".8"/>
      <text x="${sideCenter}" y="690" fill="#e2e6da" text-anchor="middle" font-size="15">≈ ${escape(formatNumber(top.count))}</text>
      <text x="${sideCenter}" y="712" fill="#b0b6a7" text-anchor="middle" font-size="11">adultos · 1 % de la población</text>
      <text x="${sideCenter}" y="780" fill="#fef702" text-anchor="middle" font-size="25" font-weight="600">${escape(formatPercent(top.share))}</text>
      <text x="${sideCenter}" y="803" fill="#b0b6a7" text-anchor="middle" font-size="11">de la riqueza nacional</text>`:'';
    const svg=`<svg id="wealthGraphic" class="moneyScene" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" data-population="${population}" data-upper="${upper}" data-lower="${lower}" data-block-unit="${unit}" data-top-columns="${stacks}" data-top-value="${top.mean}" data-split="${split}" role="group" aria-label="${escape(name)}: distribución patrimonial de ${escape(formatNumber(population))} adultos" font-family="Arial, sans-serif"><title>${escape(name)}: el patrimonio de ${escape(formatNumber(population))} adultos</title><desc>Población adulta estimada en 2024: ${escape(formatNumber(population))}. Cada columna agrupa aproximadamente ${escape(formatNumber(percentiles[0].count))} adultos y muestra su patrimonio medio. El eje horizontal expresa adultos acumulados. Los colores identifican quintiles, con sus recuentos y cuotas de riqueza. ${split?'El 1 % superior se separa y se reapila a la derecha con la misma unidad por bloque.':''} Los valores negativos quedan por debajo del cero.</desc><defs><pattern id="wealth-cash" patternUnits="userSpaceOnUse" x="${L}" y="${zero}" width="${pitch}" height="${blockHeight}"><rect x=".7" y=".65" width="${pitch-1.4}" height="${Math.max(.5,blockHeight-1.3)}" rx=".25" fill="#7fab63"/><path d="M2 1.7H${pitch-2}" stroke="#bdd4a5" stroke-width=".45" opacity=".6"/></pattern><pattern id="wealth-debt" patternUnits="userSpaceOnUse" x="${L}" y="${zero}" width="${pitch}" height="${blockHeight}"><rect x=".7" y=".65" width="${pitch-1.4}" height="${Math.max(.5,blockHeight-1.3)}" fill="#dc8c82"/></pattern><style>.wealth-interval:hover .wealth-focus,.wealth-interval:focus .wealth-focus{visibility:visible}.wealth-interval:focus,.wealth-top:focus{outline:none}.wealth-top:focus>rect{stroke:#fef702;stroke-width:1}.wealth-column,.wealth-top-column{shape-rendering:crispEdges}</style></defs><rect width="${W}" height="${H}" fill="#252825"/><text x="${L}" y="55" fill="#e9ece3" font-size="37" font-weight="700" letter-spacing="1.6">${escape(name.toLocaleUpperCase('es'))}</text><text class="wealth-population-total" x="${L}" y="80" fill="#bdc5b2" font-size="13" letter-spacing=".8">${escape(formatNumber(population))} ADULTOS DE 20 AÑOS O MÁS · 2024</text><text x="${L}" y="112" fill="#b2b9a8" font-size="13">Cada columna agrupa ≈ ${escape(formatNumber(percentiles[0].count))} adultos, ordenados por patrimonio →</text>${map}${guides}${grid}${columns}${amountLabels}${ticks}${bands}${reflow}</svg>`;
    const note=`Población adulta total: ${formatNumber(population)}. ${split?`El 1 % superior agrupa ≈ ${formatNumber(top.count)} adultos y su media se reapila en ${stacks} columnas que suman ${formatCompact(top.mean)}. `:''}Escala lineal ${common?'común a los once países':'ajustada al país'}: ${formatCompact(lower)} a ${formatCompact(upper)}. ${lowest<0?'El patrimonio negativo aparece bajo la línea de cero. ':''}Las alturas muestran medias por adulto; los recuentos de los grupos son estimaciones redondeadas.`;
    // A local origin keeps every reflowed column aligned to full-width blocks.
    const graphic=svg.replace('</defs>',`<pattern id="wealth-top-cash" patternUnits="userSpaceOnUse" x="0" y="${zero}" width="${pitch}" height="${blockHeight}"><rect x=".7" y=".65" width="${pitch-1.4}" height="${Math.max(.5,blockHeight-1.3)}" rx=".25" fill="#7fab63"/><path d="M2 1.7H${pitch-2}" stroke="#bdd4a5" stroke-width=".45" opacity=".6"/></pattern></defs>`);
    return {html:`<p class="sceneScrollHint">Desliza la gráfica ↔ para recorrer la población y el grupo superior</p><div class="wealthSceneWrap" tabindex="0" role="region" aria-label="Distribución de riqueza sobre la población adulta; desplazamiento horizontal en pantallas pequeñas">${graphic}</div><div class="sceneLegend"><span><i class="cashKey"></i>1 bloque = ${escape(formatCompact(unit))} de patrimonio medio por adulto</span><span>Bandas: quintiles${split?' · Q5 incluye el 1 % separado':' · Colores constantes entre países'}</span></div><div class="sceneActions"><p class="caption">${escape(note)}</p><button class="btn" data-view="cima">Ampliar el 1 % →</button></div>`,upper,lower,unit,stacks};
  }
  // Tail intervals are aggregates, not individual ranks: keep their means flat
  // and their widths proportional to population. Never interpolate a curve.
  function renderTop({name,population,base,intervals,allIntervals,silhouette,split=true,common=false,formatMoney,formatCompact,formatPercent,formatNumber,formatGroup}) {
    const W=1440,H=876,L=92,R=split?1186:1408,T=146,B=628,plotHeight=B-T;
    const parts=intervals.filter(g=>g.a>=base.a),top=parts.at(-1),main=split?parts.slice(0,-1):parts;
    const reference=common?allIntervals.filter(g=>g.a>=base.a&&(!split||g.group!==top.group)):main;
    const upper=ceiling(Math.max(...reference.map(g=>g.mean))*1.02),unit=upper/100;
    const y=value=>B-value/upper*plotHeight,pitch=(R-L)/100,blockHeight=unit/upper*plotHeight;
    const fractionLabel=value=>`${new Intl.NumberFormat('es-ES',{maximumFractionDigits:3}).format(value*100)} %`;
    const mainCount=main.reduce((sum,g)=>sum+g.count,0),focusLabel=fractionLabel(base.fraction),topLabel=fractionLabel(top.fraction);
    const stacks=split?Math.ceil(top.mean/upper):1,stackValue=top.mean/stacks,sideCenter=1309,stackWidth=stacks*pitch;
    const hint=g=>`${formatGroup(g.group)} · ≈ ${formatNumber(g.count)} adultos|Patrimonio medio por adulto: ${formatMoney(g.mean)}|Patrimonio total del tramo: ${formatCompact(g.total)}|${formatPercent(g.share)} de la riqueza nacional|${formatPercent(g.share/base.share)} de la riqueza del ${focusLabel} superior|Entrada desde ${formatMoney(g.lower)}${g.upper==null?' · Sin límite superior':' · Hasta '+formatMoney(g.upper)}|Media del intervalo, sin interpolación individual · WID 2024`;
    const map=silhouette?`<svg x="${L+100}" y="${T+30}" width="${R-L-150}" height="${plotHeight-35}" viewBox="0 0 1000 500" preserveAspectRatio="xMidYMid meet" aria-hidden="true"><path d="${escape(silhouette.d)}" fill="#b1b8a2" opacity=".085"/></svg>`:'';
    let grid='',ticks='',columns='',populationBands='',amounts='',cursor=L;
    for(let i=0;i<=5;i++){
      const value=upper*i/5,yy=y(value);
      grid+=`<line x1="${L}" x2="1408" y1="${yy}" y2="${yy}" stroke="${i===0?'#7b8177':'#43483f'}" stroke-width="${i===0?1.2:.8}"/><text x="${L-12}" y="${yy+4}" fill="#a1a59c" text-anchor="end" font-size="12">${escape(formatCompact(value))}</text>`;
    }
    for(let i=0;i<=10;i++){
      const x=L+(R-L)*i/10,count=mainCount*i/10;
      grid+=`<line x1="${x}" x2="${x}" y1="${T}" y2="${B}" stroke="#4a4e44" stroke-opacity=".36"/>`;
      ticks+=`<text class="wealth-population-tick" data-count="${count}" x="${x}" y="690" text-anchor="middle" fill="#b0b4a9" font-size="13">${escape(populationCompact(count))}</text>`;
    }
    ticks+=`<text x="${(L+R)/2}" y="716" text-anchor="middle" fill="#aab19f" font-size="11">Adultos acumulados dentro del ${escape(focusLabel)} superior, de menor a mayor patrimonio</text>`;
    main.forEach((g,i)=>{
      const x=cursor,w=(R-L)*g.count/mainCount,c=palette[intervals.indexOf(g)+1];cursor+=w;
      columns+=`<g class="wealth-interval wealth-tail-interval" data-group="${escape(g.group)}" data-count="${g.count}" tabindex="0" role="img" aria-label="${escape(hint(g).replaceAll('|','. '))}" data-tip="${escape(hint(g))}"><rect class="wealth-column" data-group="${escape(g.group)}" data-value="${g.mean}" x="${x}" y="${y(g.mean)}" width="${w}" height="${B-y(g.mean)}" fill="url(#wealth-cash)"/><line class="wealth-focus" x1="${x}" x2="${x}" y1="${T}" y2="641" stroke="${c}" stroke-opacity=".8" visibility="hidden"/><rect class="wealth-hit" x="${x}" y="${T}" width="${w}" height="520" fill="transparent"/></g>`;
      populationBands+=`<rect class="wealth-population-band" data-group="${escape(g.group)}" x="${x}" y="651" width="${w}" height="12" fill="${c}" opacity=".8"/>`;
      const labelX=w<160?x+w-3:x+w/2;
      amounts+=`<text x="${labelX}" y="${Math.max(T+17,y(g.mean)-12)}" text-anchor="${w<160?'end':'middle'}" fill="#d2d9c5" font-size="12">${i+1} · ${escape(formatCompact(g.mean))}</text>`;
    });
    const summaries=main.map((g,i)=>{
      const w=(R-L)/main.length,x=L+i*w,c=palette[intervals.indexOf(g)+1],cx=x+w/2;
      return `<g class="wealth-tail-summary" data-group="${escape(g.group)}" data-count="${g.count}"><rect x="${x}" y="734" width="${w-3}" height="130" fill="${c}" opacity=".04"/><line x1="${x}" x2="${x+w-3}" y1="734" y2="734" stroke="${c}" stroke-opacity=".75"/><text x="${cx}" y="756" text-anchor="middle" fill="${c}" font-size="12">${i+1} · ${escape(formatGroup(g.group))}</text><text x="${cx}" y="780" text-anchor="middle" fill="#e4e8dc" font-size="17">≈ ${escape(formatNumber(g.count))} adultos</text><text x="${cx}" y="807" text-anchor="middle" fill="${c}" font-size="22" font-weight="600">${escape(formatPercent(g.share))}</text><text x="${cx}" y="824" text-anchor="middle" fill="#aab19f" font-size="10">de la riqueza nacional</text><text x="${cx}" y="849" text-anchor="middle" fill="#d1d8c7" font-size="12">${escape(formatPercent(g.share/base.share))} de la riqueza del ${escape(focusLabel)} superior</text></g>`;
    }).join('');
    const reflow=split?`
      <line x1="1210" x2="1210" y1="30" y2="864" stroke="#555c4e" stroke-dasharray="4 6"/>
      <text x="${sideCenter}" y="35" text-anchor="middle" fill="#cdd2c4" font-size="13">EL ${escape(topLabel)} SUPERIOR</text>
      <text x="${sideCenter}" y="60" text-anchor="middle" fill="#e4e8dc" font-size="17">≈ ${escape(formatNumber(top.count))} adultos</text>
      <text x="${sideCenter}" y="93" text-anchor="middle" fill="#fef702" font-size="29" font-weight="700">${escape(formatCompact(top.mean))}</text>
      <text x="${sideCenter}" y="115" text-anchor="middle" fill="#b1b9a5" font-size="12">de patrimonio medio por adulto</text>
      <text x="${sideCenter}" y="134" text-anchor="middle" fill="#b0b6a7" font-size="10">${stacks} columnas reapiladas</text>
      ${Array.from({length:stacks},(_,i)=>`<g transform="translate(${sideCenter-stackWidth/2+i*pitch},0)"><rect class="wealth-top-column" x=".6" y="${y(stackValue)}" width="${pitch-1.2}" height="${B-y(stackValue)}" fill="url(#wealth-top-cash)"/></g>`).join('')}
      <g class="wealth-top" data-group="${escape(top.group)}" data-count="${top.count}" tabindex="0" role="img" aria-label="${escape(hint(top).replaceAll('|','. '))}" data-tip="${escape(hint(top)+'|Reapilado en '+stacks+' columnas: la suma conserva la media original.')}"><rect x="${sideCenter-stackWidth/2-8}" y="${T}" width="${stackWidth+16}" height="520" fill="transparent"/></g>
      <rect class="wealth-population-band" x="${sideCenter-stackWidth/2}" y="651" width="${stackWidth}" height="12" fill="#fef702" opacity=".8"/>
      <text x="${sideCenter}" y="690" text-anchor="middle" fill="#e2e6da" font-size="15">≈ ${escape(formatNumber(top.count))}</text>
      <text x="${sideCenter}" y="712" text-anchor="middle" fill="#b0b6a7" font-size="11">adultos · grupo separado</text>
      <text x="${sideCenter}" y="780" text-anchor="middle" fill="#fef702" font-size="25" font-weight="600">${escape(formatPercent(top.share))}</text>
      <text x="${sideCenter}" y="803" text-anchor="middle" fill="#b0b6a7" font-size="11">de la riqueza nacional</text>
      <text x="${sideCenter}" y="834" text-anchor="middle" fill="#e2e6da" font-size="17">${escape(formatPercent(top.share/base.share))}</text>
      <text x="${sideCenter}" y="853" text-anchor="middle" fill="#b0b6a7" font-size="10">de la riqueza del ${escape(focusLabel)} superior</text>`:'';
    const svg=`<svg id="wealthGraphic" class="moneyScene" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" data-kind="top" data-population="${population}" data-focus-population="${base.count}" data-focus-label="${escape(focusLabel)}" data-focus-group="${escape(base.group)}" data-upper="${upper}" data-lower="0" data-block-unit="${unit}" data-top-columns="${stacks}" data-top-value="${top.mean}" data-top-label="${escape(topLabel)}" data-split="${split}" role="group" aria-label="${escape(name)}: interior del ${escape(focusLabel)} superior, aproximadamente ${escape(formatNumber(base.count))} adultos" font-family="Arial, sans-serif">
      <title>${escape(name)}: dentro del ${escape(focusLabel)} más rico</title>
      <desc>Intervalos sin solapamiento de WID, con recuentos estimados de adultos de 20 años o más, 2024. Ancho de cada tramo proporcional a su población; altura constante igual a su patrimonio medio. Los escalones no describen diferencias individuales dentro de cada tramo. ${split?'El '+escape(topLabel)+' superior se separa a la derecha y su media se reapila sin cambiar la unidad por bloque.':''}</desc>
      <defs><pattern id="wealth-cash" patternUnits="userSpaceOnUse" x="${L}" y="${B}" width="${pitch}" height="${blockHeight}"><rect x=".7" y=".65" width="${pitch-1.4}" height="${Math.max(.5,blockHeight-1.3)}" rx=".25" fill="#7fab63"/><path d="M2 1.7H${pitch-2}" stroke="#bdd4a5" stroke-width=".45" opacity=".6"/></pattern><pattern id="wealth-top-cash" patternUnits="userSpaceOnUse" x="0" y="${B}" width="${pitch}" height="${blockHeight}"><rect x=".7" y=".65" width="${pitch-1.4}" height="${Math.max(.5,blockHeight-1.3)}" rx=".25" fill="#7fab63"/><path d="M2 1.7H${pitch-2}" stroke="#bdd4a5" stroke-width=".45" opacity=".6"/></pattern><style>.wealth-interval:hover .wealth-focus,.wealth-interval:focus .wealth-focus{visibility:visible}.wealth-interval:focus,.wealth-top:focus{outline:none}.wealth-interval:focus .wealth-hit,.wealth-top:focus>rect{stroke:#fef702;stroke-width:1}.wealth-column,.wealth-top-column{shape-rendering:crispEdges}</style></defs>
      <rect width="${W}" height="${H}" fill="#252825"/>
      <text x="${L}" y="55" fill="#e9ece3" font-size="37" font-weight="700" letter-spacing="1.6">${escape(name.toLocaleUpperCase('es'))}</text>
      <text class="wealth-population-total" x="${L}" y="80" fill="#bdc5b2" font-size="13" letter-spacing=".6">DENTRO DEL ${escape(focusLabel)} SUPERIOR · ≈ ${escape(formatNumber(base.count))} ADULTOS · 2024</text>
      <text x="${L}" y="109" fill="#d2d9c5" font-size="13">Media del grupo: ${escape(formatCompact(base.mean))} · Entrada desde ${escape(formatCompact(base.lower))} · ${escape(formatPercent(base.share))} de la riqueza nacional</text>
      <text x="${L}" y="130" fill="#aab19f" font-size="11">Eje principal: ancho = adultos · Altura = patrimonio medio por adulto · Medias constantes por intervalo</text>
      ${map}${grid}${columns}${amounts}${populationBands}${ticks}${summaries}${reflow}
    </svg>`;
    const note=`Dentro del ${focusLabel} superior: ≈ ${formatNumber(base.count)} adultos. ${split?`El ${topLabel} superior se separa: ≈ ${formatNumber(top.count)} adultos; las ${stacks} columnas reapiladas suman su media de ${formatCompact(top.mean)}. `:''}Escala lineal ${common?'común a los once países':'ajustada al país'}. Los ${parts.length} tramos son intervalos sin solapamiento; sus alturas son medias constantes, no una curva ni observaciones individuales. Los recuentos se muestran redondeados.`;
    return {html:`<p class="sceneScrollHint">Desliza la gráfica ↔ para recorrer el interior del grupo</p><div class="wealthSceneWrap" tabindex="0" role="region" aria-label="Interior del grupo más rico; desplazamiento horizontal en pantallas pequeñas">${svg}</div><div class="sceneLegend"><span><i class="cashKey"></i>1 bloque = ${escape(formatCompact(unit))} de patrimonio medio por adulto</span><span>Tramos sin solapamiento · Anchos proporcionales a la población${split?' salvo el grupo separado':''}</span></div><div class="sceneActions"><p class="caption">${escape(note)}</p><button class="btn" data-view="quintiles">← Volver al reparto</button></div>`};
  }
  return {render,renderTop};
})();
