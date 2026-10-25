import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const root = new URL('../', import.meta.url);
const study = JSON.parse(readFileSync(new URL('data/estudio.json', root), 'utf8'));
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

test('Only the three requested block amounts are offered', () => {
  const app = readFileSync(new URL('app.js', root), 'utf8');
  const allowed = app.match(/const blockUnits = (\[[^;]+\]);/);
  assert.deepEqual(JSON.parse(allowed[1]), units);
  assert.match(app, /blockUnits\.includes\(value\)/);
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

test('National layouts preserve money and block size in all 66 country/unit/mode combinations', () => {
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
        assert.equal(+attribute(html, 'data-top-value'), args.top.mean, label);
        const debtRows = Math.ceil(Math.ceil(Math.max(0, ...args.percentiles.map(g => -g.mean)) / unit) / 5) * 5;
        assert.equal(+attribute(html, 'data-lower'), debtRows ? -debtRows * unit : 0, label);
        assert.equal(+attribute(html, 'data-top-columns'), split ? Math.ceil(args.top.mean / (unit * 100)) : 1, label);
        assert.equal((html.match(/class="wealth-interval wealth-pile/g) ?? []).length, 100, label);
        for (const group of args.percentiles) {
          assert.ok(html.includes(`data-group="${group.group}" data-count="${group.count}" data-value="${group.mean}"`), label);
        }
      }
    }
  }
});

test('10,000 EUR uses US debt bounds, not Italy debt bounds, and labels sit above wrapped stacks', () => {
  const args = inputs(study.countries.find(c => c.code === 'US'), 10000);
  const html = scene.render({ ...args, split: true }).html;
  assert.equal(+attribute(html, 'data-lower'), -50000);
  assert.ok(+attribute(html, 'height') < 1000);
  assert.match(html, /class="wealth-overflow-guide" data-group="p98p99"/);
  for (const group of ['p94p95', 'p98p99']) {
    const label = html.match(new RegExp(`<text class="wealth-sample-label" data-group="${group}"[^>]*>`))[0];
    assert.ok(+attribute(label, 'y') < 166);
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
