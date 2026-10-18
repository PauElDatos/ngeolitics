import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// node oligarcas/build-maps.mjs /path/to/ne_110m_admin_0_countries.geojson
if (!process.argv[2]) throw new Error('Indica el GeoJSON de Natural Earth (admin 0, 110m).');
const source = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const codes = {ESP:'ES', USA:'US', DEU:'DE', FRA:'FR', ITA:'IT', NLD:'NL', JPN:'JP', GBR:'GB', CAN:'CA', KOR:'KR', MEX:'MX'};
const area = ring => Math.abs(ring.reduce((sum,p,i) => {const q=ring[(i+1)%ring.length]; return sum+p[0]*q[1]-q[0]*p[1];},0)/2);
const bounds = ring => [Math.min(...ring.map(p=>p[0])),Math.min(...ring.map(p=>p[1])),Math.max(...ring.map(p=>p[0])),Math.max(...ring.map(p=>p[1]))];
const countries = {};
for (const feature of source.features) {
  const code=codes[feature.properties.ADM0_A3];
  if (!code) continue;
  const polygons=feature.geometry.type==='MultiPolygon'?feature.geometry.coordinates:[feature.geometry.coordinates];
  const main=polygons.toSorted((a,b)=>area(b[0])-area(a[0]))[0];
  const mainBounds=bounds(main[0]);
  // Decorative silhouette: mainland and nearby islands, without remote territories.
  const nearby=polygons.filter(poly=>{const b=bounds(poly[0]);return b[2]>=mainBounds[0]-4&&b[0]<=mainBounds[2]+4&&b[3]>=mainBounds[1]-4&&b[1]<=mainBounds[3]+4;});
  const points=nearby.flatMap(poly=>poly[0]),b=bounds(points),cos=Math.cos((b[1]+b[3])/2*Math.PI/180);
  const width=(b[2]-b[0])*cos,height=b[3]-b[1],scale=Math.min(1000/width,500/height);
  const xPad=(1000-width*scale)/2,yPad=(500-height*scale)/2;
  countries[code]={d:nearby.flatMap(poly=>poly.map(ring=>ring.map((p,i)=>`${i?'L':'M'}${(xPad+(p[0]-b[0])*cos*scale).toFixed(1)},${(yPad+(b[3]-p[1])*scale).toFixed(1)}`).join('')+'Z')).join(''),viewBox:'0 0 1000 500'};
}
const result={source:'Natural Earth · Admin 0 countries · 110m',url:'https://www.naturalearthdata.com/',license:'Public domain',countries};
fs.writeFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)),'data/country-silhouettes.json'),JSON.stringify(result));
console.log(`${Object.keys(countries).length} siluetas de países.`);
