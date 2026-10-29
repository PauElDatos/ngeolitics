// Run with: PLAYWRIGHT_MODULE=/absolute/path/to/playwright/index.mjs node --test
// oligarcas/tests/scene-labels.browser.mjs. Serves the repository on a random port.
import assert from 'node:assert/strict';
import {before,after,test} from 'node:test';
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {resolve,extname,sep} from 'node:path';
import {fileURLToPath} from 'node:url';

const {chromium}=await import(process.env.PLAYWRIGHT_MODULE||'playwright');
const siteRoot=fileURLToPath(new URL('../../',import.meta.url)).replace(/\/$/,'');
const mime={'.html':'text/html','.js':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml'};
let server,browser,origin;
before(async()=>{
  server=createServer(async(req,res)=>{
    try{
      let pathname=decodeURIComponent(new URL(req.url,'http://localhost').pathname);
      if(pathname.endsWith('/'))pathname+='index.html';
      const path=resolve(siteRoot,'.'+pathname);
      if(!path.startsWith(siteRoot+sep)){res.writeHead(403).end();return;}
      const body=await readFile(path);
      res.writeHead(200,{'Content-Type':mime[extname(path)]||'application/octet-stream'}).end(body);
    }catch{res.writeHead(404).end();}
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  origin=`http://127.0.0.1:${server.address().port}`;
  browser=await chromium.launch({channel:process.env.BROWSER_CHANNEL||'chrome',headless:true});
});
after(async()=>{
  await browser?.close();
  if(server)await new Promise(resolve=>server.close(resolve));
});
const frames=page=>page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
async function open(page,country='ES'){
  await page.goto(`${origin}/oligarcas/index.html#quintiles?pais=${country}`);
  await page.waitForSelector('#wealthGraphic');await frames(page);
}
async function typography(page){
  return page.evaluate(()=>{
    const svg=document.querySelector('#wealthGraphic'),wrap=document.querySelector('.wealthSceneWrap');
    const layer=svg.querySelector('.wealth-readable-labels'),bounds=wrap.getBoundingClientRect();
    const issues=[];
    for(const text of layer.querySelectorAll('text')){
      const r=text.getBoundingClientRect();
      if(r.height&&r.bottom>bounds.top&&r.top<bounds.bottom){
        const size=parseFloat(getComputedStyle(text).fontSize)*text.getScreenCTM().a;
        if(size<11.9)issues.push(`Small font: ${text.textContent} (${size})`);
      }
    }
    for(const group of layer.querySelectorAll('.wealth-readable-quintile')){
      const box=group.querySelector('.wealth-quintile-card').getBoundingClientRect();
      const parts=[...group.querySelectorAll('text,.wealth-quintile-arrow')].map(node=>({name:node.textContent||'arrow',r:node.getBoundingClientRect()}));
      for(const {name,r} of parts){
        if(r.left<box.left-1||r.right>box.right+1||r.bottom>box.bottom+1)issues.push(`Card overflow: ${name}`);
      }
      for(let i=0;i<parts.length;i++)for(let j=i+1;j<parts.length;j++){
        const a=parts[i].r,b=parts[j].r;
        if(a.left<b.right&&a.right>b.left&&a.top<b.bottom&&a.bottom>b.top)issues.push(`Card overlap: ${parts[i].name} / ${parts[j].name}`);
      }
    }
    for(const text of layer.querySelectorAll('.wealth-readable-footer text')){
      const r=text.getBoundingClientRect();
      if(r.height&&r.bottom>bounds.bottom-3)issues.push(`Footer clipped: ${text.textContent}`);
    }
    const ticks=[...layer.querySelectorAll('.wealth-readable-population-tick')].filter(text=>getComputedStyle(text).display!=='none').map(text=>text.getBoundingClientRect());
    for(let i=1;i<ticks.length;i++)if(ticks[i].left<ticks[i-1].right+2)issues.push('Population tick overlap');
    return {issues,overflow:wrap.scrollWidth-wrap.clientWidth,cards:layer.querySelectorAll('.wealth-readable-quintile').length};
  });
}

test('Desktop fonts, card spacing and population ticks adapt to laptop and large display widths',async()=>{
  const page=await browser.newPage();
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  for(const [width,height] of [[1280,800],[1440,900],[1512,982],[1680,1050],[1920,1080],[2560,1440]]){
    await page.setViewportSize({width,height});
    for(const country of ['ES','US','IT']){
      await open(page,country);
      for(const mode of ['split','full']){
        await page.locator(`[data-scene-mode="${mode}"]`).click();await frames(page);
        const result=await typography(page);
        assert.deepEqual(result.issues,[],`${width}/${country}/${mode}`);
        assert.equal(result.cards,5);assert.ok(result.overflow<=1);
      }
    }
  }
  assert.deepEqual(errors,[]);await page.close();
});

test('Every country retains unclipped captions in both national layouts',async()=>{
  const page=await browser.newPage({viewport:{width:1440,height:900}});
  await open(page);
  const countries=await page.locator('.country').evaluateAll(nodes=>nodes.map(node=>node.dataset.country));
  for(const country of countries){
    await page.locator(`.country[data-country="${country}"]`).click();await frames(page);
    for(const mode of ['split','full']){
      await page.locator(`[data-scene-mode="${mode}"]`).click();await frames(page);
      assert.deepEqual((await typography(page)).issues,[],`${country}/${mode}`);
    }
  }
  await page.close();
});

test('Country headings have no background and do not intercept the bars beneath them',async()=>{
  const page=await browser.newPage({viewport:{width:1680,height:1050}});
  await open(page,'US');
  assert.equal(await page.locator('.wealth-readable-heading rect,#wealthSceneHeading rect').count(),0);
  assert.equal(await page.locator('.wealth-readable-heading text').count(),2);
  const group=await page.evaluate(()=>{
    const bar=document.querySelector('.wealth-pile[data-group="p98p99"] .wealth-column').getBoundingClientRect();
    const plot=document.querySelector('.wealthSceneWrap').getBoundingClientRect();
    return document.elementFromPoint(bar.left+bar.width/2,plot.top+58)?.closest('.wealth-pile')?.dataset.group;
  });
  assert.equal(group,'p98p99','The bar remains interactive in the former heading background area');
  await page.locator('.sectionTabs [data-view="cima"]').click();await frames(page);
  assert.equal(await page.locator('#wealthSceneHeading rect').count(),0);
  assert.equal(await page.locator('#wealthSceneHeading text').count(),3);
  await page.close();
});

test('Responsive quintiles keep keyboard dialogs, isolated scrolling and summit/base navigation',async()=>{
  const page=await browser.newPage({viewport:{width:1680,height:1050}});
  await open(page);
  const q5=page.locator('.wealth-readable-quintile[data-quintile-detail="p80p100"]');
  await q5.focus();await q5.press('Enter');
  await page.waitForSelector('#quintileDetailDialog[open]');
  assert.match(await page.locator('#quintileDetailTitle').textContent(),/Q5/);
  await page.keyboard.press('Escape');
  await page.waitForFunction(()=>!document.querySelector('#quintileDetailDialog').open);
  const wrap=page.locator('.wealthSceneWrap');
  await wrap.focus();await wrap.press('End');await frames(page);
  const initial=await page.evaluate(()=>({top:+document.querySelector('#wealthGraphic').dataset.windowTop,main:document.querySelector('#main').scrollTop,heading:document.querySelector('.wealth-readable-heading').getBoundingClientRect().top}));
  await wrap.hover();await page.mouse.wheel(0,600);await frames(page);
  assert.equal(await page.locator('#main').evaluate(node=>node.scrollTop),initial.main);
  assert.equal(await page.locator('#wealthGraphic').evaluate(node=>+node.dataset.windowTop),initial.top);
  await page.mouse.wheel(0,-240);await page.waitForFunction(top=>+document.querySelector('#wealthGraphic').dataset.windowTop<top,initial.top);
  assert.equal(await page.locator('#main').evaluate(node=>node.scrollTop),initial.main);
  assert.equal(await page.locator('.wealth-readable-heading').evaluate(node=>node.getBoundingClientRect().top),initial.heading);
  await wrap.press('Home');await frames(page);
  assert.equal(await page.locator('.wealth-readable-footer').getAttribute('display'),'none');
  const summit=await page.locator('.wealth-readable-summit').boundingBox();
  const plot=await wrap.boundingBox();
  assert.ok(summit.y>=plot.y&&summit.y+summit.height<plot.y+plot.height);
  // Resize while at the summit, then return to base: hidden text must still be
  // measured correctly, rather than treating its widths as zero.
  await page.setViewportSize({width:1440,height:900});await frames(page);
  await wrap.press('End');await frames(page);
  assert.deepEqual((await typography(page)).issues,[]);
  await page.setViewportSize({width:2560,height:1440});await frames(page);
  assert.deepEqual((await typography(page)).issues,[]);
  assert.equal(await page.locator('#wealthGraphic').getAttribute('data-block-unit'),'10000');
  await page.close();
});

test('Phone layout and its separate readable HTML captions remain unchanged',async()=>{
  for(const [width,height] of [[390,844],[430,932],[844,390]]){
    const context=await browser.newContext({viewport:{width,height},isMobile:true,hasTouch:true});
    const page=await context.newPage();await open(page);
    assert.equal(await page.locator('.mobileWealthScene').count(),1);
    assert.equal(await page.locator('.wealth-readable-labels').count(),0);
    assert.equal(await page.locator('.mobileQuintileRow').count(),5);
    assert.equal(await page.locator('#wealthGraphic').getAttribute('data-block-unit'),'10000');
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
    const zoom=page.locator('.mobileSceneZoom');await zoom.click();
    assert.equal(await zoom.getAttribute('aria-pressed'),'true');await zoom.click();
    assert.equal(await zoom.getAttribute('aria-pressed'),'false');
    await page.locator('.mobileQuintileRow[data-quintile-detail="p80p100"]').click();
    await page.waitForSelector('#quintileDetailDialog[open]');
    await page.locator('.quintileDialogClose').click();
    await context.close();
  }
});
