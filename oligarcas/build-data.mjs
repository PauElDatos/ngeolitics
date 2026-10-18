import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

// Rebuild: node oligarcas/build-data.mjs /absolute/path/to/source.json
const input = process.argv[2];
if (!input) throw new Error('Indica la ruta al JSON original de la investigación.');
const raw = fs.readFileSync(input);
const d = JSON.parse(raw);
const fields = { share: 'wealth_share', mean: 'mean_wealth', median: 'median_wealth', total: 'total_wealth', lower: 'lower_threshold', upper: 'upper_threshold', count: 'population_count' };
const amount = x => x?.eur_market?.value ?? x?.value ?? null;
const groups = d.wid_detailed_2024.groups.map(g => ({
  country: g.country, period: g.period, category: g.category, group: g.group,
  a: g.lower_percentile, b: g.upper_percentile, fraction: g.population_fraction,
  currency: g.currency, source: g.source_reference,
  ...Object.fromEntries(Object.entries(fields).map(([key, field]) => [key, ['share', 'count'].includes(key) ? g[field]?.value ?? null : amount(g[field])])),
  provenance: Object.fromEntries(Object.entries(fields).map(([key, field]) => [key, {
    status: g[field]?.status ?? null, input_ids: g[field]?.input_ids ?? [],
    formula: g[field]?.formula ?? null, original: g[field]?.value ?? null,
  }])),
}));
const national = d.wid_national.map(n => ({
  country: n.country, period: n.period, currency: n.currency,
  mean: amount(n.distribution_mean_equal_split), median: amount(n.median_net_wealth_equal_split),
  total: amount(n.aggregate_net_wealth), population: n.adult_population.value,
  gini: n.gini_equal_split?.value ?? null,
  provenance: Object.fromEntries(['distribution_mean_equal_split', 'median_net_wealth_equal_split', 'aggregate_net_wealth', 'gini_equal_split'].map(k => [k, n[k]])),
}));
const official = d.official_observations.map(o => Object.fromEntries([
  'id', 'country', 'period', 'group', 'indicator', 'value', 'unit', 'currency', 'statistical_unit',
  'ranking_variable', 'source_reference', 'source_url', 'wealth_definition_id', 'method', 'component',
  'dataset_family', 'price_basis', 'observation_date', 'limitations',
].filter(k => o[k] !== undefined).map(k => [k, o[k]])));
const output = {
  metadata: d.metadata, source_sha256: createHash('sha256').update(raw).digest('hex'),
  definitions: d.wid_detailed_2024.definitions, countries: d.countries, groups, national, official,
  official_definitions: d.official_definitions, coverage: d.coverage_2025_2026,
  sources: d.sources, fx: { policy: d.foreign_exchange.policy, rates: d.foreign_exchange.rates.filter(r => r.period === '2025') },
  validation: d.validation, source_coverage: d.source_specific_coverage,
};
const dir = path.dirname(fileURLToPath(import.meta.url));
fs.mkdirSync(path.join(dir, 'data'), { recursive: true });
fs.writeFileSync(path.join(dir, 'data/estudio.json'), JSON.stringify(output));
const csv = ['country,period,category,group,population_fraction,wealth_share,mean_EUR_2025,median_EUR_2025,total_EUR_2025,lower_EUR_2025,upper_EUR_2025,adults,source', ...groups.map(g => [g.country, g.period, g.category, g.group, g.fraction, g.share, g.mean, g.median, g.total, g.lower, g.upper, g.count, g.source].map(v => v ?? '').join(','))].join('\n');
fs.writeFileSync(path.join(dir, 'data/grupos_2024.csv'), '\uFEFF' + csv);
console.log(`${groups.length} grupos, ${national.length} agregados y ${official.length} observaciones oficiales. JSON: ${(fs.statSync(path.join(dir, 'data/estudio.json')).size / 1048576).toFixed(2)} MB.`);
