'use strict';

// Monetary geometry stays in scene units. This annotation layer uses CSS pixels,
// so fitting the bars to a laptop never shrinks their captions into tiny print.
window.WealthSceneLabels=(()=>{
  const NS='http://www.w3.org/2000/svg';
  const number=new Intl.NumberFormat('es-ES',{notation:'compact',maximumFractionDigits:2});
  const node=(tag,attrs={},parent)=>{
    const element=document.createElementNS(NS,tag);
    for(const [key,value] of Object.entries(attrs))element.setAttribute(key,value);
    parent?.append(element);return element;
  };
  const at=(element,attrs)=>{for(const [key,value] of Object.entries(attrs))element.setAttribute(key,value);};
  function label(parent,source,size,anchor='middle'){
    const text=node('text',{'font-size':size,fill:source?.getAttribute('fill')||'#bdc5b2','text-anchor':anchor},parent);
    if(source?.hasAttribute('font-weight'))text.setAttribute('font-weight',source.getAttribute('font-weight'));
    text.textContent=source?.textContent||'';text.dataset.label=text.textContent;return text;
  }
  function lines(text,value,x,y,width,size,lineHeight=size+3){
    at(text,{x,y,'font-size':size,'aria-label':value});text.replaceChildren();
    const words=value.split(/\s+/);let current='',row=0;
    const span=()=>node('tspan',{x,dy:row++?lineHeight:0},text);
    let part=span();
    for(const word of words){
      const next=current?current+' '+word:word;part.textContent=next;
      if(current&&part.getComputedTextLength()>width){part.textContent=current;part=span();current=word;part.textContent=word;}
      else current=next;
    }
    return row*lineHeight;
  }
  function fit(text,value,width,size,minimum){
    text.textContent=value;at(text,{'font-size':size});
    if(text.getComputedTextLength()>width)size=Math.max(minimum,size*width/text.getComputedTextLength());
    text.setAttribute('font-size',size);return size;
  }
  function mount(svg,{left=92}={}){
    const content=svg.querySelector('#wealthSceneContent');
    const sourceHeading=svg.querySelector('#wealthSceneHeading');
    const sourceTop=svg.querySelector('#wealthSeparatedHeading');
    const panel=svg.querySelector('#wealthRichestPanel');
    const originals=[...svg.querySelectorAll('.wealth-quintile')];
    const moneyBottom=+originals[0].querySelector('rect').getAttribute('y')-23;
    const layer=node('g',{class:'wealth-readable-labels'},svg);
    // Do not leave duplicate keyboard controls or duplicated accessible labels.
    sourceHeading.setAttribute('display','none');sourceHeading.setAttribute('aria-hidden','true');
    content.querySelectorAll('text').forEach(text=>text.style.display='none');
    const heading=node('g',{class:'wealth-readable-heading'},layer),headingBackground=node('rect',{rx:8,fill:'#252825','fill-opacity':'.95'},heading);
    const title=label(heading,sourceHeading.querySelector('text'),30,'start');
    const population=label(heading,sourceHeading.querySelectorAll('text')[1],12,'start');
    const axis=[...content.querySelectorAll('.wealth-axis-label')].map(source=>({source,text:label(layer,source,12,'end')}));
    const samples=[...content.querySelectorAll('.wealth-sample-label')].map(source=>({source,text:label(layer,source,12,source.getAttribute('text-anchor'))}));
    const footer=node('g',{class:'wealth-readable-footer'},layer);
    const cards=originals.map(original=>{
      const band=original.querySelector('rect'),box=original.querySelector('.wealth-quintile-card');
      const x=+band.getAttribute('x'),width=+box.getAttribute('width');
      const texts=[...original.querySelectorAll('text')];
      const color=texts[0].getAttribute('fill');
      const group=original.cloneNode(false);group.classList.add('wealth-readable-quintile');footer.append(group);
      original.setAttribute('display','none');original.setAttribute('aria-hidden','true');original.setAttribute('tabindex','-1');
      const stripe=node('rect',{fill:color,opacity:.8},group);
      const background=node('rect',{class:'wealth-quintile-card',fill:color,opacity:.04},group);
      const rule=node('line',{stroke:color,'stroke-opacity':.75},group);
      const captions=texts.map((text,i)=>label(group,text,i===3?23:i===1?14:12));
      captions[1].classList.add('wealth-quintile-count');
      const arrow=original.querySelector('.wealth-quintile-arrow').cloneNode(true);group.append(arrow);
      return {original,x,width,texts,group,stripe,background,rule,captions,arrow};
    });
    const tickSources=[...content.querySelectorAll('.wealth-population-tick')];
    const ticks=tickSources.map(source=>{
      const text=label(footer,source,12);text.classList.add('wealth-readable-population-tick');
      return {source,text};
    });
    const captionSource=content.querySelector('.wealth-population-caption');
    const caption=label(footer,captionSource,12);
    const mainEnd=Math.max(...cards.map(card=>card.x+card.width+3));
    const topGroup=sourceTop?node('g',{class:'wealth-readable-top'},layer):null;
    let topBackground,topTexts,topSources,topShare;
    if(sourceTop){
      sourceTop.setAttribute('display','none');sourceTop.setAttribute('aria-hidden','true');
      topBackground=node('rect',{rx:8,fill:'#252825','fill-opacity':'.95'},topGroup);
      topSources=[...sourceTop.querySelectorAll('text')];
      topTexts=topSources.map((source,i)=>label(topGroup,source,i===2?25:12));
      topShare=[...content.querySelectorAll('.wealth-top-share text')].map((source,i)=>label(footer,source,i?12:25));
    }
    const richest=panel?node('g',{class:'wealth-readable-richest'},footer):null;
    const richestSources=panel?[...panel.querySelectorAll('text')].slice(-4):[];
    const richestTexts=richestSources.map((source,i)=>label(richest,source,i===1?18:12));
    const summit=panel?node('g',{class:'wealth-readable-summit'},layer):null;
    const summitTexts=panel?[...panel.querySelectorAll('text')].slice(0,3).map((source,i)=>label(summit,source,i===1?18:12)):[];
    const virtual=node('g',{},layer);
    // Keep the fixed heading above annotations, but never over the euro axis.
    layer.append(heading);
    const cardY=78,cardHeight=176;
    let footerHeight=270;
    let layoutKey='';
    return {
      gutter:88,
      bottom:zoom=>moneyBottom+footerHeight/zoom,
      top:zoom=>panel?Math.min(+svg.dataset.logicalTop,+panel.dataset.top-140/zoom):+svg.dataset.logicalTop,
      paint({zoom,top,viewport,pixels,width,visibleWidth,inset,scrollLeft,bottom,virtualTicks}){
        at(layer,{transform:`translate(${-inset/zoom},0) scale(${1/zoom})`});
        const x=value=>value*zoom+inset;
        const moneyY=(moneyBottom-top)*zoom;
        const footerDisplay=moneyY>pixels||moneyY+footerHeight<0?'none':'inline';
        // Measuring hidden SVG text returns zero, including when resized at the
        // summit. Measure first, then hide off-screen keyboard controls.
        at(footer,{transform:`translate(0,${moneyY+18})`,display:'inline'});
        if(topGroup)at(topGroup,{transform:`translate(0,${(Math.max(0,bottom-viewport)-top)*zoom})`});
        const zero=+svg.dataset.zero;
        for(const {source,text} of axis){
          const y=(+source.getAttribute('y')-4-top)*zoom;
          const crowded=+source.getAttribute('y')-4>zero&&(+source.getAttribute('y')-4-zero)*zoom<20;
          at(text,{x:x(left)-12,y:y+4,display:y<12||y>pixels-8||crowded?'none':'inline'});
        }
        virtual.replaceChildren();
        for(const tick of virtualTicks){
          const text=label(virtual,null,12,'end');text.textContent=tick.label;
          at(text,{x:x(left)-12+scrollLeft,y:tick.y*zoom+4});
        }
        // Place sample values in screen space and resolve collisions at that size.
        const placed=[];
        const headingBottom=Number(headingBackground.getAttribute('y'))+Number(headingBackground.getAttribute('height'))+12;
        for(const {source,text} of samples){
          text.setAttribute('display','inline');
          let y=(+source.getAttribute('y')-top)*zoom;
          const xx=x(+source.getAttribute('x')),w=text.getComputedTextLength();
          const start=source.getAttribute('text-anchor')==='end'?xx-w:xx-w/2;
          while(placed.some(p=>start<p.end+6&&start+w>p.start-6&&Math.abs(y-p.y)<18))y-=18;
          placed.push({start,end:start+w,y});
          const behindHeading=y<headingBottom;
          at(text,{x:xx,y,display:y<14||y>pixels-8||behindHeading?'none':'inline'});
        }
        if(summit){
          const yy=(+panel.dataset.top-top)*zoom;
          const joined=panel.dataset.joined==='true';
          const xx=x(+panel.dataset.x)+(joined?16:5*zoom);
          const available=joined?width-xx-8:Math.max(80,(width-xx-8)*2);
          let baseline=yy-76;
          summitTexts.forEach((text,i)=>{
            text.setAttribute('text-anchor',joined?'start':'middle');
            baseline+=lines(text,text.dataset.label,xx,baseline,available,i===1?18:12)+3;
          });
          summit.setAttribute('display',yy<0||yy-90>pixels?'none':'inline');
        }
        const key=[zoom,width,inset,scrollLeft].join('/');
        if(key===layoutKey){footer.setAttribute('display',footerDisplay);return;}layoutKey=key;
        const headingX=x(left)+12+scrollLeft;
        const headingWidth=Math.max(160,Math.min((mainEnd-left)*zoom-24,visibleWidth-x(left)-24));
        const headingSize=Math.max(26,Math.min(34,width/28));
        const titleHeight=lines(title,sourceHeading.querySelector('text').textContent,headingX,36,headingWidth,headingSize,headingSize+3);
        const subtitleY=36+titleHeight+1;
        const subtitleHeight=lines(population,sourceHeading.querySelectorAll('text')[1].textContent,headingX,subtitleY,headingWidth,12,16);
        at(headingBackground,{x:headingX-8,y:10,width:headingWidth+16,height:subtitleY+subtitleHeight-12});
        const countSize=Math.min(...cards.map(card=>card.width*zoom))<115?12:15;
        const compactCounts=cards.some(card=>{
          const count=card.captions[1];count.textContent=card.texts[1].textContent;count.setAttribute('font-size',countSize);
          return count.getComputedTextLength()>card.width*zoom-8;
        });
        for(const card of cards){
          const xx=x(card.x),w=card.width*zoom,cx=xx+w/2;
          at(card.stripe,{x:xx,y:0,width:w,height:6});
          at(card.background,{x:xx,y:cardY,width:w,height:cardHeight});
          at(card.rule,{x1:xx,x2:xx+w,y1:cardY,y2:cardY});
          lines(card.captions[0],card.texts[0].textContent,cx,cardY+32,w-8,12,14);
          if(card.captions[0].children.length===1)card.captions[0].setAttribute('y',cardY+38);
          const count=card.captions[1];count.textContent=compactCounts?'≈ '+number.format(+card.original.dataset.count):card.texts[1].textContent;
          at(count,{x:cx,y:cardY+68,'font-size':countSize,'aria-label':card.texts[1].textContent});
          lines(card.captions[2],card.texts[2].textContent,cx,cardY+88,w-8,12,14);
          fit(card.captions[3],card.texts[3].textContent,w-8,24,16);
          at(card.captions[3],{x:cx,y:cardY+134});
          lines(card.captions[4],card.texts[4].textContent,cx,cardY+154,w-6,12,14);
          at(card.arrow,{transform:`translate(${xx+w-11},${cardY+10}) scale(.7)`});
        }
        // Endpoints are retained; intermediate population ticks are thinned only
        // when their full counts would collide. No data or percentile is removed.
        ticks.forEach(tick=>tick.text.setAttribute('display','inline'));
        const end=ticks.at(-1);let previous=-Infinity;
        end.text.textContent=end.source.textContent;
        const lastLeft=x(+end.source.getAttribute('x'))-end.text.getComputedTextLength()/2;
        for(const [index,tick] of ticks.entries()){
          const xx=x(+tick.source.getAttribute('x')),w=tick.text.getComputedTextLength();
          const keep=tick===end||(index%2===0&&xx-w/2>previous+10&&xx+w/2<lastLeft-10);
          at(tick.text,{x:xx,y:30,display:keep?'inline':'none'});
          if(keep)previous=xx+w/2;
        }
        lines(caption,captionSource.textContent,x((left+mainEnd)/2),51,(mainEnd-left)*zoom-16,12,15);
        if(topGroup){
          const rect=sourceTop.querySelector('rect');
          const xx=x(+rect.getAttribute('x')),w=+rect.getAttribute('width')*zoom,cx=xx+w/2;
          let baseline=29;
          topTexts.forEach((text,i)=>{
            const value=topSources[i].textContent;
            const size=i===2?fit(text,value,w-8,26,20):i===1?14:12;
            baseline+=lines(text,value,cx,baseline,w-8,size,size+4)+7;
          });
          at(topBackground,{x:xx,y:12,width:w,height:baseline-10});
          fit(topShare[0],topShare[0].dataset.label,w-8,25,18);
          at(topShare[0],{x:cx,y:cardY+59});
          lines(topShare[1],topShare[1].dataset.label,cx,cardY+81,w-8,12,16);
        }
        if(richest){
          const joined=panel.dataset.joined==='true';
          const xx=x(+panel.dataset.x)+(joined?16:5*zoom);
          const divider=panel.querySelector('.wealth-richest-divider');
          const w=joined?width-xx-8:width-x(+divider.getAttribute('x1'))-12;
          let baseline=cardY+28;
          richestTexts.forEach((text,i)=>{
            text.setAttribute('text-anchor',joined?'start':'middle');
            const value=richestSources[i].textContent;
            const size=i===1?fit(text,value,w-8,19,14):12;
            baseline+=lines(text,value,xx,baseline,w-8,size,i===1?23:16)+3;
          });
        }
        footerHeight=Math.max(270,Math.ceil(footer.getBBox().y+footer.getBBox().height)+32);
        footer.setAttribute('display',footerDisplay);
      },
    };
  }
  return {mount};
})();
