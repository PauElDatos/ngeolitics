import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const root=new URL('../',import.meta.url);
const study=JSON.parse(readFileSync(new URL('data/estudio.json',root),'utf8'));
const context=vm.createContext({window:{}});
vm.runInContext(readFileSync(new URL('quintile-detail.js',root),'utf8'),context);
const {thresholds}=context.window.QuintileDetail;

test('All 55 quintile curves preserve the published boundaries, including zeros and negative wealth',()=>{
  for(const country of study.countries){
    const groups=study.groups.filter(group=>group.country===country.code);
    const percentiles=groups.filter(group=>group.category==='percentile');
    const cumulative=groups.filter(group=>group.category==='cumulative');
    for(const group of groups.filter(group=>group.category==='quintile')){
      const points=thresholds(group,percentiles,cumulative);
      assert.ok(points.length>=20,`${country.code}/${group.group}`);
      assert.equal(points[0].p,group.a);
      assert.equal(points[0].value,group.lower);
      for(let i=0;i<points.length;i++){
        assert.ok(Number.isFinite(points[i].value));
        assert.ok(points[i].p>=group.a&&points[i].p<=group.b);
        if(i)assert.ok(points[i].p>points[i-1].p);
      }
      if(group.upper!=null){
        assert.equal(points.at(-1).p,group.b);
        assert.equal(points.at(-1).value,group.upper);
      }
    }
  }
});

test('An open Q5 has no invented P100 maximum and retains the finest published top thresholds',()=>{
  for(const country of study.countries){
    const groups=study.groups.filter(group=>group.country===country.code);
    const quintile=groups.find(group=>group.category==='quintile'&&group.b===100);
    const cumulative=groups.filter(group=>group.category==='cumulative');
    const points=thresholds(quintile,groups.filter(group=>group.category==='percentile'),cumulative);
    assert.equal(quintile.upper,null);
    assert.ok(points.every(point=>point.p<100));
    for(const top of cumulative.filter(group=>group.a>=99)){
      const point=points.find(point=>point.p===top.a);
      assert.ok(point,`${country.code}/P${top.a}`);
      assert.equal(point.value,top.lower);
    }
    assert.equal(points.at(-1).p,99.999);
  }
});
