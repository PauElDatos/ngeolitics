'use strict';

window.WealthScene = (() => {
  const palette=['#67b6df','#69c7ae','#bdcd73','#eeb55e','#fef702'];
  const ROWS=100,DX=10,DY=5,L=92;
  let sceneObserver;
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
  function frame({width,height,name,population,unit,T,zero,debtRows,silhouette,formatCompact,attributes,header,extraHeader='',body,description,positiveRows=ROWS,singleBars=false,richestValue=0}) {
    const logicalTop=Math.min(0,zero-Math.ceil(richestValue/unit)*DY-65);
    return `<svg id="wealthGraphic" class="moneyScene fixedBlockScene" xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" style="--scene-width:${width}px" data-logical-top="${logicalTop}" data-logical-bottom="${height}" data-zero="${zero}" data-population="${population}" data-block-unit="${unit}" data-stack-capacity="${unit*ROWS}" data-block-rows="${ROWS}" data-plot-rows="${positiveRows}" data-block-width="8" data-block-height="${DY-1}" data-row-step="${DY}" data-upper="${unit*positiveRows}" data-lower="${-debtRows*unit}" ${attributes} role="group" aria-label="${escape(name)}: patrimonio en bloques monetarios de valor fijo" font-family="Arial, sans-serif"><title>${escape(name)}: bloques de ${escape(precise(unit))}</title><desc>${escape(description)} Un bloque verde o dorado tiene idéntico tamaño y vale ${escape(precise(unit))}. La fortuna Forbes utiliza los mismos bloques: una barra vertical en el reparto general y torres laterales de 100 bloques dentro del 1 %. La cifra original se publica en USD de 2026 y la conversión a EUR es nominal, no EUR de 2025. ${singleBars?'Cada intervalo del eje principal forma una sola barra vertical.':'Las columnas laterales completas contienen 100 bloques.'} Las marcas naranjas son restos menores que un bloque, no unidades completas.</desc>${defs()}<rect id="wealthBackground" width="${width}" height="${height}" fill="#252825"/><g id="wealthVirtualGrid"></g><g id="wealthSceneContent">${map(silhouette,zero-ROWS*DY,width)}${grid(width,zero,unit,debtRows,formatCompact,positiveRows)}${body}${extraHeader}</g><g id="wealthRichestVisible"></g><g id="wealthSceneHeading"><rect x="${L-16}" y="18" width="790" height="112" rx="8" fill="#252825" fill-opacity=".94"/>${header}</g></svg>`;
  }
  function reference(unit,formatMoney) {
    return `<div class="blockReference"><span><i class="cashKey"></i>= ${escape(formatMoney(unit))}</span><span class="blockRemainderKey"><i></i>resto menor que un bloque</span><span><i class="richestKey"></i>fortuna Forbes 2026</span></div>`;
  }
  function wrap(svg,unit,formatMoney,actions,{singleBars=false,scrollable=false,split=false,richest=false,tail=false}={}) {
    scrollable=scrollable||richest;
    const viewport=`<div class="wealthSceneWrap${tail?' tailSceneWrap':scrollable?' verticalSceneWrap':''}${singleBars&&!split?' joinedSceneWrap':''}" tabindex="0" role="region" aria-label="Pilas monetarias de tamaño fijo; desplazamiento horizontal${tail?'':' y vertical'}">${svg}</div>`;
    if(singleBars)return `<div class="nationalScene">${viewport}<aside class="sceneSidebar" aria-label="Controles y leyenda del reparto">${actions?`<section class="sceneControlGroup"><h3>Vista del reparto</h3><div class="sceneViewControls" role="group" aria-label="Disposición y ampliación de las barras">${actions}</div></section>`:''}<section class="sceneLegendGroup"><h3>Leyenda</h3>${reference(unit,formatMoney)}</section><section class="sceneControlGroup"><h3>Recorrer la altura</h3><div class="fixedSceneNavigation"><button class="btn" data-scene-scroll="top">↑ Ir a la cima</button><button class="btn" data-scene-scroll="base">↓ Ir a la base</button></div></section></aside></div>`;
    return reference(unit,formatMoney)+viewport+`<div class="sceneActions">${actions}</div>`;
  }

  // Use the same local origin and geometry as pile(), so a block is never split
  // across two horizontal pattern tiles. The full height remains in scene units.
  function richestPanel({person,source,x,T,zero,panelY,height,unit,formatMoney,formatCompact,formatNumber,joined=false,labelsX,separatorX=x-20}) {
    if(!person)return '';
    const amount=person.usd_billions*1e9/source.usd_per_eur;
    const totalBlocks=amount/unit,blocks=Math.floor(totalBlocks),remainder=amount-blocks*unit,realHeight=totalBlocks*DY;
    const cx=x+DX/2,labelX=labelsX??(joined?x+30:cx),topLabelX=joined?x+30:cx,anchor=joined?'start':'middle',usd=formatNumber(person.usd_billions*1000)+' M$';
    const top=zero-Math.ceil(totalBlocks)*DY;
    const details=`${person.name} · Forbes, 1-3-2026|USD originales: ${formatNumber(person.usd_billions*1000)} M$|Conversión a EUR nominales: ${formatCompact(amount)}; 1 € = ${source.usd_per_eur} $ (BCE, 27-2-2026)|${formatNumber(blocks)} bloques enteros de ${formatMoney(unit)}${remainder?' + resto '+precise(remainder):''}|Una sola barra vertical: misma anchura y altura por bloque que los verdes|Según ciudadanía Forbes, no residencia. No es WID 2024 ni EUR de 2025${person.includes_family?'|Forbes atribuye la cifra al hombre y su familia':''}`;
    return `<g id="wealthRichestPanel" class="wealth-richest" data-name="${escape(person.name)}" data-usd-billions="${person.usd_billions}" data-eur-nominal="${amount}" data-total-blocks="${totalBlocks}" data-true-height="${realHeight}" data-source-year="2026" data-x="${x}" data-top="${top}" data-joined="${joined}" tabindex="0" role="img" aria-label="${escape(details.replaceAll('|','. '))}" data-tip="${escape(details)}">${joined?'':`<line class="wealth-richest-divider" x1="${separatorX}" x2="${separatorX}" y1="30" y2="${height-12}" stroke="#8c7542" stroke-dasharray="4 6"/>`}<g class="wealth-richest-stack" transform="translate(${x},${zero})"><rect class="wealth-richest-column" data-blocks="${blocks}" x="0" y="${-blocks*DY}" width="${DX}" height="${blocks*DY}" fill="url(#wealth-richest-cash)"/>${remainder?`<rect class="wealth-remainder" data-value="${remainder}" x="1" y="${-blocks*DY-DY+.5}" width="8" height="${DY-1}" fill="url(#wealth-rest)" stroke="#f3b865" stroke-width=".6"/>`:''}</g><text x="${topLabelX}" y="${top-50}" text-anchor="${anchor}" fill="#e8ede2" font-size="13">${escape(person.name)}</text><text x="${topLabelX}" y="${top-30}" text-anchor="${anchor}" fill="#ffd766" font-size="17">${usd}</text><text x="${topLabelX}" y="${top-10}" text-anchor="${anchor}" fill="#efd696" font-size="12">≈ ${escape(formatCompact(amount))}</text><text x="${labelX}" y="${panelY+22}" text-anchor="${anchor}" fill="#e8ede2" font-size="13">${escape(person.name)}</text><text x="${labelX}" y="${panelY+44}" text-anchor="${anchor}" fill="#ffd766" font-size="18" font-weight="700">${usd}</text><text x="${labelX}" y="${panelY+64}" text-anchor="${anchor}" fill="#efd696" font-size="12">≈ ${escape(formatCompact(amount))}</text><text x="${labelX}" y="${panelY+82}" text-anchor="${anchor}" fill="#bcb8a8" font-size="11">Forbes 2026 · una persona${person.includes_family?' y familia':''}</text></g>`;
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

  function richestTowers({person,source,x,zero,panelY,unit,formatMoney,formatCompact,formatNumber}) {
    if(!person)return {html:'',width:0};
    const amount=person.usd_billions*1e9/source.usd_per_eur,blocks=Math.floor(amount/unit),remainder=amount-blocks*unit;
    const fullColumns=Math.floor(blocks/ROWS),lastBlocks=blocks%ROWS,hasLast=lastBlocks>0||remainder>0;
    const columns=fullColumns+(hasLast?1:0),width=columns*DX;
    const usd=formatNumber(person.usd_billions*1000)+' M$';
    const tip=`${person.name} · Forbes 2026|USD originales: ${usd}|Equivalencia nominal: ${formatMoney(amount)}|${formatNumber(fullColumns)} torres completas de ${ROWS} bloques${hasLast?' + última torre con '+formatNumber(lastBlocks)+' bloques enteros':''}${remainder?' + resto '+precise(remainder):''}|Un bloque = ${formatMoney(unit)}|1 € = ${source.usd_per_eur} $ · BCE, 27-2-2026${person.includes_family?'|Forbes atribuye la cifra al hombre y su familia':''}`;
    const full=fullColumns?`<rect class="wealth-richest-full-towers" data-columns="${fullColumns}" x="0" y="${-ROWS*DY}" width="${fullColumns*DX}" height="${ROWS*DY}" fill="url(#wealth-richest-cash)"/>`:'';
    const last=hasLast?`<g class="wealth-richest-last-tower" transform="translate(${fullColumns*DX},0)"><rect class="wealth-richest-column" x="0" y="${-lastBlocks*DY}" width="${DX}" height="${lastBlocks*DY}" fill="url(#wealth-richest-cash)"/>${remainder?`<rect class="wealth-remainder" data-value="${remainder}" x="1" y="${-lastBlocks*DY-DY+.5}" width="8" height="${DY-1}" fill="url(#wealth-rest)" stroke="#f3b865" stroke-width=".6"/>`:''}</g>`:'';
    return {width,html:`<g id="wealthRichestPanel" class="wealth-richest" data-layout="wrapped" data-name="${escape(person.name)}" data-usd-billions="${person.usd_billions}" data-eur-nominal="${amount}" data-total-blocks="${amount/unit}" data-full-columns="${fullColumns}" data-last-blocks="${lastBlocks}" data-remainder="${remainder}" data-column-count="${columns}" data-x="${x}" tabindex="0" role="img" aria-label="${escape(tip.replaceAll('|','. '))}" data-tip="${escape(tip)}"><line x1="${x-10}" x2="${x-10}" y1="138" y2="${panelY+130}" stroke="#8c7542" stroke-dasharray="4 6"/><text x="${x+16}" y="151" fill="#e7c766" font-size="13">${escape(person.name)}</text><text x="${x+16}" y="176" fill="#e9ece3" font-size="20" font-weight="600">${usd} · ≈ ${escape(formatCompact(amount))}</text><g class="wealth-richest-stack" transform="translate(${x},${zero})">${full}${last}<rect x="0" y="${-ROWS*DY}" width="${width}" height="${ROWS*DY}" fill="transparent"/></g><rect x="${x}" y="${zero+23}" width="${width}" height="8" fill="#e7c766" opacity=".8"/><text x="${x+16}" y="${panelY+22}" fill="#e7c766" font-size="16">${escape(person.name)}</text><text x="${x+16}" y="${panelY+49}" fill="#ffd766" font-size="22" font-weight="600">${usd}</text><text x="${x+16}" y="${panelY+73}" fill="#e4e8dc" font-size="14">≈ ${escape(formatCompact(amount))}</text><text x="${x+16}" y="${panelY+100}" fill="#aab19f" font-size="11">${escape(formatNumber(columns))} torres · Forbes 2026</text></g>`};
  }

  function render({name,population,percentiles,quintiles,top,allPercentiles,silhouette,split=true,unit=10000,richestMan,richestSource,sceneControls='',formatMoney,formatCompact,formatPercent,formatNumber}) {
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
    const positions=main.map(g=>{const width=DX,pos={g,x:cursor,width,single:true};cursor+=width;return pos;});
    const mainRight=cursor,sideX=mainRight+32;
    const topPack=decompose(top.mean,unit),sideWidth=Math.max(180,topPack.columns.length*DX+48);
    const topDividerX=sideX-24,richestDividerX=sideX+sideWidth+76;
    const width=Math.max(1600,split?richestDividerX+182:mainRight+140);
    const topCenter=(topDividerX+richestDividerX)/2;
    const richestX=split?(richestDividerX+width-32)/2-DX/2:mainRight;
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
    let extraHeader='';
    if(split){
      const cx=topCenter;
      body+=`<line class="wealth-top-divider" x1="${topDividerX}" x2="${topDividerX}" y1="30" y2="${height-12}" stroke="#555c4e" stroke-dasharray="4 6"/>`;
      extraHeader=`<g id="wealthSeparatedHeading"><rect x="${topDividerX+8}" y="18" width="${richestDividerX-topDividerX-16}" height="112" rx="8" fill="#252825" fill-opacity=".94"/><text x="${cx}" y="35" text-anchor="middle" fill="#cdd2c4" font-size="13">EL 1 % SUPERIOR</text><text class="wealth-top-count" x="${cx}" y="60" text-anchor="middle" fill="#e4e8dc" font-size="17">≈ ${escape(formatNumber(top.count))} adultos</text><text x="${cx}" y="93" text-anchor="middle" fill="#fef702" font-size="29" font-weight="700">${escape(formatCompact(top.mean))}</text><text x="${cx}" y="115" text-anchor="middle" fill="#b1b9a5" font-size="11">${topPack.fullColumns} completas${topPack.columns.length>topPack.fullColumns?' + última con resto':''}</text></g>`;
      body+=pile(top,cx-topPack.columns.length*DX/2,zero,unit,hint(top,false),true);
      body+=`<text x="${cx}" y="${panelY+44}" fill="#fef702" text-anchor="middle" font-size="25" font-weight="600">${escape(formatPercent(top.share))}</text><text x="${cx}" y="${panelY+67}" fill="#b0b6a7" text-anchor="middle" font-size="11">de la riqueza nacional</text>`;
    }
    body+=richestPanel({person:richestMan,source:richestSource,x:richestX,T,zero,panelY,height,unit,formatMoney,formatCompact,formatNumber,joined:!split,separatorX:richestDividerX});
    const header=`<text x="${L}" y="55" fill="#e9ece3" font-size="37" font-weight="700" letter-spacing="1.6">${escape(name.toLocaleUpperCase('es'))}</text><text class="wealth-population-total" x="${L}" y="80" fill="#bdc5b2" font-size="13">${escape(formatNumber(population))} ADULTOS DE 20 AÑOS O MÁS · 2024</text><text x="${L}" y="112" fill="#b2b9a8" font-size="13">Cada intervalo agrupa ≈ ${escape(formatNumber(percentiles[0].count))} adultos, ordenados por patrimonio →</text>`;
    const svg=frame({width,height,name,population,unit,T,zero,debtRows,silhouette,formatCompact,header,extraHeader,body,positiveRows,singleBars:true,richestValue:richestMan?richestMan.usd_billions*1e9/richestSource.usd_per_eur:0,attributes:`data-kind="national" data-main-layout="single" data-split="${split}" data-top-layout="${split?'wrapped':'single'}" data-top-columns="${split?topPack.columns.length:1}" data-unfolded-top-columns="${topPack.columns.length}" data-top-value="${top.mean}" data-scene-main-end="${split?sideX:mainRight-DX-1}" ${richestMan?'data-richest-x="'+richestX+'"':''}`,description:'Intervalos del 1 % de adultos, con su patrimonio medio. La población se indica en cifras, no por el número de columnas monetarias.'});
    return {html:wrap(svg,unit,formatMoney,sceneControls,{singleBars:true,scrollable:positiveRows>ROWS,split,richest:!!richestMan})};
  }

  function renderTop({name,population,base,intervals,silhouette,unit=10000,richestMan,richestSource,formatMoney,formatCompact,formatPercent,formatNumber,formatGroup}) {
    const T=188,zero=T+ROWS*DY,bandY=zero+23,panelY=bandY+42,height=panelY+142;
    const parts=intervals.filter(g=>g.a>=base.a),top=parts.at(-1),focusLabel=fraction(base.fraction),topLabel=fraction(top.fraction);
    const widths=parts.map(g=>Math.max(248,decompose(g.mean,unit).columns.length*DX+32));
    const spare=Math.max(0,1440-L-32-widths.reduce((a,b)=>a+b,0));
    let cursor=L;
    const positions=parts.map((g,i)=>{const width=widths[i]+spare/parts.length,pos={g,x:cursor,width};cursor+=width;return pos;});
    const richestX=cursor+24;
    const towers=richestTowers({person:richestMan,source:richestSource,x:richestX,zero,panelY,unit,formatMoney,formatCompact,formatNumber});
    const width=richestMan?richestX+Math.max(400,towers.width)+32:cursor+32;
    let body='';
    positions.forEach(({g,x,width:w},i)=>{
      const c=palette[intervals.indexOf(g)+1],tx=x+16;
      const tip=`${formatGroup(g.group)} · ≈ ${formatNumber(g.count)} adultos|Patrimonio medio por adulto: ${formatMoney(g.mean)}|${packing(g.mean,unit,formatNumber,formatMoney)}|Patrimonio total del tramo: ${formatCompact(g.total)}|${formatPercent(g.share)} de la riqueza nacional|${formatPercent(g.share/base.share)} de la riqueza del ${focusLabel} superior|Entrada desde ${formatMoney(g.lower)}${g.upper==null?' · Sin límite superior':' · Hasta '+formatMoney(g.upper)}|Intervalo sin interpolación individual · WID 2024`;
      body+=`<line x1="${x-1}" x2="${x-1}" y1="138" y2="${height-12}" stroke="#555c4e" stroke-dasharray="4 6"/><text x="${tx}" y="151" fill="${c}" font-size="13">${i+1} · ${escape(formatGroup(g.group))}</text><text x="${tx}" y="176" fill="#e9ece3" font-size="20" font-weight="600">${escape(formatCompact(g.mean))} por adulto</text>`;
      body+=pile(g,tx,zero,unit,tip,i===parts.length-1);
      body+=`<rect x="${x}" y="${bandY}" width="${w-3}" height="8" fill="${c}" opacity=".8"/><g class="wealth-tail-summary" data-group="${escape(g.group)}" data-count="${g.count}"><rect x="${x}" y="${panelY}" width="${w-3}" height="130" fill="${c}" opacity=".04"/><text x="${tx}" y="${panelY+22}" fill="#e4e8dc" font-size="14">≈ ${escape(formatNumber(g.count))} adultos</text><text x="${tx}" y="${panelY+49}" fill="${c}" font-size="22" font-weight="600">${escape(formatPercent(g.share))}</text><text x="${tx}" y="${panelY+65}" fill="#aab19f" font-size="10">de la riqueza nacional</text><text x="${tx}" y="${panelY+89}" fill="#d1d8c7" font-size="12">${escape(formatPercent(g.share/base.share))} de la riqueza del ${escape(focusLabel)} superior</text><text x="${tx}" y="${panelY+113}" fill="#aab19f" font-size="11">${escape(formatCompact(g.mean))} por adulto</text></g>`;
    });
    body+=towers.html;
    const header=`<text x="${L}" y="55" fill="#e9ece3" font-size="37" font-weight="700" letter-spacing="1.6">${escape(name.toLocaleUpperCase('es'))}</text><text class="wealth-population-total" x="${L}" y="80" fill="#bdc5b2" font-size="13">DENTRO DEL ${escape(focusLabel)} SUPERIOR · ≈ ${escape(formatNumber(base.count))} ADULTOS · 2024</text><text x="${L}" y="109" fill="#d2d9c5" font-size="13">Media del grupo: ${escape(formatCompact(base.mean))} · Entrada desde ${escape(formatCompact(base.lower))} · ${escape(formatPercent(base.share))} de la riqueza nacional</text>`;
    const topPack=decompose(top.mean,unit);
    const svg=frame({width,height,name,population,unit,T,zero,debtRows:0,silhouette,formatCompact,header,body,attributes:`data-kind="top" data-focus-population="${base.count}" data-focus-label="${escape(focusLabel)}" data-focus-group="${escape(base.group)}" data-top-label="${escape(topLabel)}" data-top-columns="${topPack.columns.length}" data-top-value="${top.mean}" data-split="true" data-richest-x="${richestX}"`,description:'Tramos exclusivos del 1 % y fortuna individual en torres laterales de 100 bloques de 10.000 €. Cada pila WID muestra el patrimonio medio por adulto de su tramo; su ancho monetario no representa población.'});
    return {html:wrap(svg,unit,formatMoney,'<button class="btn" data-view="quintiles">← Volver al reparto</button>',{tail:true})};
  }
  // A fortune can exceed the browser's maximum element height. Keep the scene
  // in monetary coordinates and move a fixed-size SVG window over its full
  // extent; the native scrollbar navigates that extent without scaling blocks.
  function mount(root) {
    sceneObserver?.disconnect();
    const wrap=root.querySelector('.wealthSceneWrap'),svg=root.querySelector('#wealthGraphic');
    if(!wrap||!svg)return;
    if(svg.dataset.kind==='top')return;
    const logicalTop=Number(svg.dataset.logicalTop),bottom=Number(svg.dataset.logicalBottom);
    const sceneWidth=Number(svg.getAttribute('width'));
    const scale=()=>window.innerWidth>900?wrap.clientWidth/sceneWidth:1;
    const surface=document.createElement('div');
    surface.className='wealthSceneSurface';surface.style.width=svg.getAttribute('width')+'px';
    wrap.classList.add('virtualSceneWrap');svg.before(surface);surface.append(svg);
    const logicalHeight=bottom-logicalTop,physicalHeight=Math.min(1000000,logicalHeight);
    surface.style.height=physicalHeight+'px';
    const content=svg.querySelector('#wealthSceneContent'),separatedHeading=svg.querySelector('#wealthSeparatedHeading'),panel=svg.querySelector('#wealthRichestPanel'),gold=svg.querySelector('#wealthRichestVisible');
    const zero=Number(svg.dataset.zero),unit=Number(svg.dataset.blockUnit);
    const worldTop=Math.min(0,...[...svg.querySelectorAll('.wealth-pile')].map(g=>zero-Math.ceil(Math.max(0,Number(g.dataset.value))/unit)*(g.dataset.layout==='single'?DY:0)-20));
    const topLabels=panel?[...panel.querySelectorAll('text')].filter(t=>Number(t.getAttribute('y'))<Number(svg.dataset.logicalTop)+100):[];
    if(panel){
      panel.querySelector('.wealth-richest-stack').setAttribute('display','none');
      topLabels.forEach(t=>t.setAttribute('display','none'));
      gold.setAttribute('class','wealth-richest');gold.setAttribute('data-tip',panel.dataset.tip);
    }
    let offset=0,lastScroll=-1,viewport=0,pinnedToBase=true;
    function paint(){
      const zoom=scale(),pixels=wrap.clientHeight;
      viewport=pixels/zoom;
      svg.setAttribute('height',pixels);svg.style.height=pixels+'px';
      svg.style.width=sceneWidth*zoom+'px';svg.style.minWidth=sceneWidth*zoom+'px';surface.style.width=sceneWidth*zoom+'px';
      const y=logicalTop+offset;
      svg.setAttribute('viewBox',`0 0 ${svg.getAttribute('width')} ${viewport}`);
      svg.dataset.windowTop=y;svg.dataset.windowBottom=y+viewport;
      svg.querySelector('#wealthSceneHeading').setAttribute('transform',`translate(${wrap.scrollLeft/zoom},0)`);
      if(separatedHeading)separatedHeading.setAttribute('transform',`translate(0,${Math.max(0,bottom-viewport)})`);
      svg.querySelector('#wealthBackground').setAttribute('height',viewport);
      content.setAttribute('display',y+viewport<worldTop?'none':'inline');
      content.setAttribute('transform',`translate(0,${-y})`);
      const rows=Number(svg.dataset.plotRows),width=Number(svg.getAttribute('width'));
      const first=Math.ceil((zero-y-viewport)/(DY*ROWS)),last=Math.floor((zero-y)/(DY*ROWS));
      let extra='';
      for(let n=Math.max(first,Math.ceil(rows/ROWS));n<=last;n++){
        const yy=zero-n*DY*ROWS-y,value=n*ROWS*unit;
        const axisX=Math.max(L-12,wrap.scrollLeft/zoom+80);
        extra+=`<line x1="${L}" x2="${width-32}" y1="${yy}" y2="${yy}" stroke="#43483f" stroke-width=".8"/><text x="${axisX}" y="${yy+4}" fill="#a1a59c" text-anchor="end" font-size="12">${escape(new Intl.NumberFormat('es-ES',{maximumFractionDigits:2}).format(value/1e6))} M€</text>`;
      }
      svg.querySelector('#wealthVirtualGrid').innerHTML=extra;
      if(panel){
        const amount=Number(panel.dataset.eurNominal),blocks=Math.floor(amount/unit),remainder=amount-blocks*unit,x=Number(panel.dataset.x);
        const firstRow=Math.max(-blocks,Math.floor((y-zero)/DY)),lastRow=Math.min(0,Math.ceil((y+viewport-zero)/DY));
        const start=zero+firstRow*DY-y,marker=zero-(blocks+1)*DY+.5-y;
        let visible=lastRow>firstRow?`<g transform="translate(${x},${start})"><rect class="wealth-richest-column" x="0" y="0" width="${DX}" height="${(lastRow-firstRow)*DY}" fill="url(#wealth-richest-cash)"/></g>`:'';
        if(remainder&&marker>-DY&&marker<viewport)visible+=`<rect class="wealth-remainder" x="${x+1}" y="${marker}" width="8" height="${DY-1}" fill="url(#wealth-rest)" stroke="#f3b865" stroke-width=".6"/>`;
        for(const label of topLabels){
          const localY=Number(label.getAttribute('y'))-y;
          if(localY<0||localY>viewport)continue;
          const clone=label.cloneNode(true);clone.removeAttribute('display');clone.setAttribute('y',localY);visible+=clone.outerHTML;
        }
        gold.innerHTML=visible;
      }
    }
    function move(next){
      viewport=wrap.clientHeight/scale();
      offset=Math.max(0,Math.min(logicalHeight-viewport,next));
      pinnedToBase=offset>=logicalHeight-viewport-.001;
      wrap.scrollTop=offset/Math.max(1,logicalHeight-viewport)*Math.max(0,physicalHeight-wrap.clientHeight);
      lastScroll=wrap.scrollTop;paint();
    }
    wrap.addEventListener('scroll',()=>{
      if(Math.abs(wrap.scrollTop-lastScroll)<.1){paint();return;}
      lastScroll=wrap.scrollTop;
      offset=wrap.scrollTop/Math.max(1,physicalHeight-wrap.clientHeight)*Math.max(0,logicalHeight-viewport);
      pinnedToBase=wrap.scrollTop>=physicalHeight-wrap.clientHeight-.1;
      paint();
    });
    wrap.addEventListener('wheel',event=>{
      if(event.ctrlKey||event.shiftKey||Math.abs(event.deltaX)>Math.abs(event.deltaY))return;
      const delta=event.deltaY*(event.deltaMode===1?16/scale():event.deltaMode===2?viewport:1/scale());
      const next=Math.max(0,Math.min(logicalHeight-viewport,offset+delta));
      if(next===offset)return;
      event.preventDefault();move(next);
    },{passive:false});
    wrap.addEventListener('keydown',event=>{
      const deltas={ArrowUp:-40,ArrowDown:40,PageUp:-viewport*.9,PageDown:viewport*.9,Home:-logicalHeight,End:logicalHeight};
      if(event.key in deltas){event.preventDefault();move(offset+deltas[event.key]);}
    });
    wrap.navigate=action=>{
      if(action==='top'){move(0);wrap.scrollLeft=Math.max(0,Number(svg.dataset.richestX)-wrap.clientWidth/2);paint();}
      else if(action==='base')move(logicalHeight);
      else if(action==='richest')wrap.scrollLeft=Math.max(0,Number(svg.dataset.richestX)-wrap.clientWidth/2);
      else wrap.scrollLeft=action==='end'?Math.max(0,Number(svg.dataset.sceneMainEnd)-wrap.clientWidth/2):0;
    };
    sceneObserver=new ResizeObserver(()=>move(pinnedToBase?logicalHeight:offset));sceneObserver.observe(wrap);
    move(logicalHeight);
  }
  return {render,renderTop,decompose,mount};
})();
