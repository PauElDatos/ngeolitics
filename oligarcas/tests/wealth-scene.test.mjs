import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const root = new URL('../', import.meta.url);
const study = JSON.parse(readFileSync(new URL('data/estudio.json', root), 'utf8'));
const richest = JSON.parse(readFileSync(new URL('data/hombres-mas-ricos-2026.json', root), 'utf8'));
const context = vm.createContext({ window: {} });
vm.runInContext(readFileSync(new URL('wealth-scene.js', root), 'utf8'), context);
const scene = context.window.WealthScene;
const units = [10000, 50000, 100000];
const allPercentiles = study.groups.filter(g => g.category === 'percentile');
const number = v => new Intl.NumberFormat('es-ES', { maximumFractionDigits: 0 }).format(v);
const format = {
  formatMoney: v => `${number(v)} €`,
  formatCompact: v => `${number(v)} €`,
  formatPercent: v => `${(v * 100).toFixed(2)} %`,
  formatNumber: number,
  formatGroup: String,
};

function inputs(country, unit) {
  const groups = study.groups.filter(g => g.country === country.code);
  return {
    ...format, name: country.name, unit,
    population: study.national.find(n => n.country === country.code && n.period === 2024).population,
    percentiles: groups.filter(g => g.category === 'percentile').sort((a, b) => a.a - b.a),
    quintiles: groups.filter(g => g.category === 'quintile'),
    top: groups.find(g => g.category === 'cumulative' && g.group === 'p99p100'),
    intervals: groups.filter(g => g.category === 'top_nonoverlapping').sort((a, b) => a.a - b.a),
    allPercentiles,
  };
}

function attribute(html, name) {
  const match = html.match(new RegExp(` ${name}="([^"]+)"`));
  assert.ok(match, `Missing ${name}`);
  return match[1];
}

test('The application fixes every scene at 10,000 EUR without a selector', () => {
  const app = readFileSync(new URL('app.js', root), 'utf8');
  assert.match(app, /const BLOCK_UNIT = 10000;/);
  assert.doesNotMatch(app, /id="blockUnit"|state\.blockUnit|blockUnits/);
  assert.equal(richest.source.usd_per_eur, 1.1805);
  assert.deepEqual(Object.keys(richest.countries).sort(),study.countries.map(c=>c.code).sort());
});

test('Complete columns and remainders preserve every amount at all three units', () => {
  for (const unit of units) {
    for (const value of [0, unit - .25, unit, unit * 100, unit * 100 + .25, -482429.8, 17089892.196899]) {
      const parts = scene.decompose(value, unit);
      const sum = parts.columns.reduce((total, column, index) => {
        assert.equal(column.value, column.blocks * unit + column.remainder);
        assert.ok(column.blocks <= 100);
        assert.ok(column.remainder >= 0 && column.remainder < unit);
        if (index < parts.fullColumns) {
          assert.equal(column.blocks, 100);
          assert.equal(column.remainder, 0);
        }
        return total + column.value;
      }, 0);
      assert.ok(Math.abs(sum - Math.abs(value)) < .001);
      assert.equal(parts.negative, value < 0);
    }
  }
});

test('All 66 national layouts grow every principal interval in a single vertical bar', () => {
  for (const country of study.countries) {
    for (const unit of units) {
      for (const split of [true, false]) {
        const args = inputs(country, unit);
        const html = scene.render({ ...args, split }).html;
        const label = `${country.code}/${unit}/${split}`;
        assert.doesNotMatch(html, /NaN|Infinity|undefined/, label);
        assert.equal(+attribute(html, 'data-block-unit'), unit, label);
        assert.equal(+attribute(html, 'data-stack-capacity'), unit * 100, label);
        assert.equal(+attribute(html, 'data-block-height'), 4, label);
        assert.equal(+attribute(html, 'data-row-step'), 5, label);
        assert.equal(attribute(html, 'data-main-layout'), 'single', label);
        assert.equal(+attribute(html, 'data-top-value'), args.top.mean, label);
        const debtRows = Math.ceil(Math.ceil(Math.max(0, ...args.percentiles.map(g => -g.mean)) / unit) / 5) * 5;
        assert.equal(+attribute(html, 'data-lower'), debtRows ? -debtRows * unit : 0, label);
        assert.equal(+attribute(html, 'data-top-columns'), split ? Math.ceil(args.top.mean / (unit * 100)) : 1, label);
        assert.equal((html.match(/class="wealth-interval wealth-pile/g) ?? []).length, 100, label);
        const reference = allPercentiles.filter(g => !split || g.b < 100);
        const positiveRows = Math.max(100, Math.ceil(Math.max(...reference.map(g => g.mean)) / (unit * 100)) * 100);
        assert.equal(+attribute(html, 'data-plot-rows'), positiveRows, label);
        const piles = [...html.matchAll(/<g class="wealth-interval wealth-pile(?: wealth-top)?"[^>]+>/g)].map(m => m[0]);
        for (const group of args.percentiles) {
          const pile = piles.find(p => attribute(p, 'data-group') === group.group);
          assert.equal(+attribute(pile, 'data-value'), group.mean, label);
          const single = !split || group.b < 100;
          assert.equal(attribute(pile, 'data-layout'), single ? 'single' : 'wrapped', label);
          if (single) {
            assert.equal(+attribute(pile, 'data-column-count'), group.mean === 0 ? 0 : 1, label);
            const blockHeight = Math.floor(Math.abs(group.mean) / unit) * 5;
            const y = group.mean < 0 ? 0 : -blockHeight;
            if (group.mean !== 0) assert.ok(html.includes(`data-group="${group.group}" data-value="${group.mean}" x="0" y="${y}" width="10" height="${blockHeight}"`), `${label}/${group.group}: full vertical height`);
            assert.ok(Math.ceil(Math.max(0, group.mean) / unit) <= positiveRows, `${label}: bar fits vertically`);
          }
        }
      }
    }
  }
});

test('10,000 EUR never wraps a principal bar after 100 blocks in either mode', () => {
  const args = inputs(study.countries.find(c => c.code === 'US'), 10000);
  for (const split of [true, false]) {
    const html = scene.render({ ...args, split }).html;
    assert.equal(+attribute(html, 'data-lower'), -50000);
    assert.ok(+attribute(html, 'data-plot-rows') >= 400);
    assert.match(html, /verticalSceneWrap/);
    assert.doesNotMatch(html, /wealth-overflow-guide/);
    for (const id of ['p94p95', 'p98p99']) {
      const pile = html.match(new RegExp(`<g class="wealth-interval wealth-pile"[^>]*data-group="${id}"[^>]*>`))[0];
      assert.equal(attribute(pile, 'data-layout'), 'single');
      assert.equal(+attribute(pile, 'data-column-count'), 1);
      assert.ok(+attribute(pile, 'data-block-count') > 100);
      assert.doesNotMatch(pile, /columnas completas/);
    }
  }
});

test('All 99 country/unit/depth top-tail layouts retain exact values and 100-block columns', () => {
  const ids = ['p99p100', 'p99.9p100', 'p99.99p100'];
  for (const country of study.countries) {
    for (const unit of units) {
      const args = inputs(country, unit);
      for (const id of ids) {
        const base = study.groups.find(g => g.country === country.code && g.category === 'cumulative' && g.group === id);
        const html = scene.renderTop({ ...args, base }).html;
        assert.doesNotMatch(html, /NaN|Infinity|undefined/);
        assert.equal(+attribute(html, 'data-block-unit'), unit);
        assert.equal(+attribute(html, 'data-stack-capacity'), unit * 100);
        assert.equal(+attribute(html, 'data-block-height'), 4);
        for (const group of args.intervals.filter(g => g.a >= base.a)) {
          assert.ok(html.includes(`data-group="${group.group}" data-count="${group.count}" data-value="${group.mean}"`));
        }
      }
    }
  }
});

test('Every country keeps complete fortunes in the same 10k-block scene, with joined bars adjacent', () => {
  for (const country of study.countries) {
    const args=inputs(country,10000),person=richest.countries[country.code];
    assert.ok(person.name && person.usd_billions>0,country.code);
    const common={...args,richestMan:person,richestSource:richest.source};
    const base=study.groups.find(g=>g.country===country.code && g.category==='cumulative' && g.group==='p99p100');
    for(const html of [
      scene.render({...common,split:true}).html,
      scene.render({...common,split:false}).html,
      scene.renderTop({...common,base}).html,
      scene.renderTop({...common,base,split:false}).html,
    ]){
      assert.equal(+attribute(html,'data-block-unit'),10000,country.code);
      const panel=html.match(/<g id="wealthRichestPanel"[^>]+>/)?.[0];
      assert.ok(panel,country.code);
      assert.equal(attribute(panel,'data-name'),person.name.replaceAll('&','&amp;'),country.code);
      const amount=person.usd_billions*1e9/richest.source.usd_per_eur;
      assert.equal(+attribute(panel,'data-eur-nominal'),amount,country.code);
      assert.equal(+attribute(panel,'data-total-blocks'),amount/10000,country.code);
      assert.equal(+attribute(panel,'data-true-height'),amount/10000*5,country.code);
      assert.match(html,/fill="url\(#wealth-richest-cash\)"/,country.code);
      assert.doesNotMatch(html,/wealthOverview|Escala completa|gráfica superior/,country.code);
      assert.ok(+attribute(html,'data-logical-top')<+attribute(panel,'data-top'),country.code);
      const gold=html.match(/<rect class="wealth-richest-column"[^>]+>/)?.[0];
      assert.equal(+attribute(gold,'height'),Math.floor(amount/10000)*5,country.code);
      assert.equal(+attribute(gold,'width'),10,country.code);
      assert.equal(+attribute(gold,'x'),0,country.code);
      assert.equal(+attribute(gold,'y'),-Math.floor(amount/10000)*5,country.code);
      if(attribute(panel,'data-joined')==='true'){
        const last=[...html.matchAll(/<g class="wealth-stack-column"[^>]+>/g)].at(-1)[0];
        const lastX=+attribute(last,'transform').match(/translate\(([^,]+)/)[1];
        assert.equal(+attribute(panel,'data-x')-lastX,11,country.code);
        assert.equal(+attribute(gold,'height'),Math.floor(amount/10000)*5);
      }
      const patterns=['wealth-cash','wealth-richest-cash'].map(id=>html.match(new RegExp(`<pattern id="${id}"[^>]*>(.*?)</pattern>`))[1]);
      const geometry=p=>p.replace(/fill="[^"]+"|stroke="[^"]+"|opacity="[^"]+"/g,'');
      assert.equal(geometry(patterns[0]),geometry(patterns[1]),country.code);
      assert.doesNotMatch(html,/rotura|interrumpida|data-scale-break/,country.code);
      assert.match(html,/Persona más rica →/,country.code);
      assert.doesNotMatch(html,/referencia, no límite de altura/,country.code);
    }
  }
});
