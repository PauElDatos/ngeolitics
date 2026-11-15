(() => {
  'use strict';
  const data = window.REFORMA_DATA;
  if (!data) return;
  const $ = id => document.getElementById(id);
  const colors = { actual: '#e3e3e3', vox: '#FEF702', a: '#67b6df', b: '#69c7ae', c: '#eeb55e' };
  const labels = { actual: 'Actual', vox: 'VOX', a: 'TIPO A', b: 'TIPO B', c: 'TIPO C' };
  const visible = new Set(Object.keys(labels));
  const number = (n, digits = 0) => new Intl.NumberFormat('es-ES', { minimumFractionDigits: digits, maximumFractionDigits: digits, useGrouping: true }).format(n);
  const money = (n, digits = 0) => `${number(n, digits)} €`;
  const percent = (n, digits = 2) => `${number(n * 100, digits)} %`;
  const el = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const svgEl = (tag, attrs, text) => {
    const node = document.createElementNS('http://www.w3.org/2000/svg', tag);
    Object.entries(attrs || {}).forEach(([key, value]) => node.setAttribute(key, value));
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const shortTitles = { savings: 'Ahorro anual', net: 'Variación del neto', rate: 'Tipo efectivo', share: 'Ahorro sobre el bruto' };
  const selectedChart = id => data.charts.find(c => c.id === id);
  let chart = selectedChart('savings');
  const plot = $('plot'), slider = $('salary'), output = $('salary-value'), values = $('values');
  let svg, cursor, markers, metrics;
  let salary = 30; // Thousands of euros, shared across chart selections.
  const notes = {
    savings: 'Diferencia respecto al IRPF actual. Un valor negativo significa que se paga más IRPF. Hasta 150.000 € de salario bruto.',
    net: 'Cambio del sueldo neto respecto al modelo actual. Hasta 150.000 € de salario bruto.',
    rate: 'IRPF dividido entre salario bruto anual. Es el tipo efectivo, no el marginal. Hasta 500.000 € de salario bruto.',
    share: 'Ahorro anual dividido entre salario bruto anual. Hasta 150.000 € de salario bruto.'
  };

  const formatValue = value => chart.id === 'savings' ? money(value, 2) : percent(value);
  function inspect(x) {
    salary = Math.max(Number(slider.min), Math.min(Number(slider.max), Math.round(x)));
    slider.value = salary;
    slider.setAttribute('aria-valuetext', money(salary * 1000));
    output.textContent = money(salary * 1000);
    values.replaceChildren();
    if (!metrics) return;
    const px = metrics.sx(salary);
    cursor.setAttribute('x1', px);
    cursor.setAttribute('x2', px);
    markers.replaceChildren();
    chart.series.filter(s => visible.has(s.id)).forEach(series => {
      // Each chart's embedded cache contains one observed value per 1,000 €.
      const point = series.points.reduce((best, p) => Math.abs(p[0] - salary) < Math.abs(best[0] - salary) ? p : best);
      const item = el('div', 'chart-value');
      item.style.setProperty('--series-color', colors[series.id]);
      item.append(el('span', 'series-label', labels[series.id]), el('strong', '', formatValue(point[1])));
      values.append(item);
      markers.append(svgEl('circle', { cx: metrics.sx(point[0]), cy: metrics.sy(point[1]), r: 3.5, fill: colors[series.id], stroke: '#252525', 'stroke-width': 1.5 }));
    });
    if (chart.id !== 'rate' && visible.has('actual')) {
      const ref = el('div', 'chart-value');
      ref.style.setProperty('--series-color', colors.actual);
      ref.append(el('span', 'series-label', 'Actual · referencia'), el('strong', '', formatValue(0)));
      values.prepend(ref);
    }
  }
  function draw() {
    const width = Math.max(1, plot.clientWidth);
    const height = Math.max(1, plot.clientHeight);
    const margin = { left: chart.id === 'savings' ? 62 : 48, right: 16, top: 16, bottom: 49 };
    const [xmin, xmax] = chart.xDomain;
    const [ymin, ymax] = chart.yDomain;
    const sx = x => margin.left + (x - xmin) / (xmax - xmin) * (width - margin.left - margin.right);
    const sy = y => height - margin.bottom - (y - ymin) / (ymax - ymin) * (height - margin.top - margin.bottom);
    metrics = { sx, sy };
    svg = svgEl('svg', { viewBox: `0 0 ${width} ${height}`, role: 'img', 'aria-labelledby': `svg-title-${chart.id} svg-desc-${chart.id}`, class: 'line-chart' });
    svg.append(svgEl('title', { id: `svg-title-${chart.id}` }, chart.title));
    svg.append(svgEl('desc', { id: `svg-desc-${chart.id}` }, 'Comparación de los modelos seleccionados. Eje horizontal: salario bruto anual en miles de euros. Usa el control de salario bajo la gráfica para consultar los valores con el teclado.'));
    const clipId = `clip-${chart.id}`;
    const defs = svgEl('defs');
    const clip = svgEl('clipPath', { id: clipId });
    clip.append(svgEl('rect', { x: sx(xmin), y: sy(ymax), width: sx(xmax) - sx(xmin), height: sy(ymin) - sy(ymax) }));
    defs.append(clip);
    svg.append(defs);
    const ySteps = chart.id === 'share' ? (height > 380 ? 10 : 5) : (height > 380 ? 8 : 4);
    for (let i = 0; i <= ySteps; i++) {
      const y = ymin + (ymax - ymin) * i / ySteps;
      svg.append(svgEl('line', { x1: sx(xmin), x2: sx(xmax), y1: sy(y), y2: sy(y), class: Math.abs(y) < 1e-10 ? 'zero-line' : 'grid-line' }));
      svg.append(svgEl('text', { x: margin.left - 10, y: sy(y) + 4, 'text-anchor': 'end', class: 'axis-label' }, chart.id === 'savings' ? number(y) : percent(y, 0)));
    }
    if (chart.id === 'share' && ySteps % 2) svg.append(svgEl('line', { x1: sx(xmin), x2: sx(xmax), y1: sy(0), y2: sy(0), class: 'zero-line' }));
    const xTicks = width < 420 ? (xmax === 500 ? [0, 100, 250, 400, 500] : [0, 50, 100, 150]) : (xmax === 500 ? [0, 100, 200, 300, 400, 500] : [0, 25, 50, 75, 100, 125, 150]);
    xTicks.forEach(x => {
      svg.append(svgEl('line', { x1: sx(x), x2: sx(x), y1: sy(ymin), y2: sy(ymax), class: 'grid-line vertical-grid' }));
      svg.append(svgEl('text', { x: sx(x), y: height - 29, 'text-anchor': 'middle', class: 'axis-label' }, number(x)));
    });
    svg.append(svgEl('text', { x: (sx(xmin) + sx(xmax)) / 2, y: height - 7, 'text-anchor': 'middle', class: 'axis-title' }, 'Salario bruto anual (miles de €)'));
    const lines = svgEl('g', { 'clip-path': `url(#${clipId})` });
    chart.series.filter(s => visible.has(s.id)).forEach(series => {
      const path = series.points.map((p, i) => `${i ? 'L' : 'M'}${sx(p[0]).toFixed(2)},${sy(p[1]).toFixed(2)}`).join(' ');
      lines.append(svgEl('path', { d: path, fill: 'none', stroke: colors[series.id], 'stroke-width': 2.5, 'stroke-linejoin': 'round', 'stroke-linecap': 'round', class: 'chart-series', 'data-scenario': series.id }));
    });
    cursor = svgEl('line', { y1: sy(ymax), y2: sy(ymin), class: 'chart-cursor' });
    markers = svgEl('g');
    lines.append(cursor, markers);
    svg.append(lines);
    const overlay = svgEl('rect', { x: sx(xmin), y: sy(ymax), width: sx(xmax) - sx(xmin), height: sy(ymin) - sy(ymax), fill: 'transparent', class: 'chart-overlay' });
    overlay.addEventListener('pointermove', event => {
      if (event.pointerType === 'touch') return;
      const rect = svg.getBoundingClientRect();
      const localX = (event.clientX - rect.left) / rect.width * width;
      inspect(xmin + (localX - margin.left) / (width - margin.left - margin.right) * (xmax - xmin));
    });
    svg.append(overlay);
    plot.replaceChildren(svg);
    inspect(salary);
  }

  function renderThumbnails() {
    data.charts.forEach(c => {
      const btn = $('pick-' + c.id);
      btn.setAttribute('aria-pressed', String(c.id === chart.id));
      const mini = svgEl('svg', { viewBox: '0 0 144 52', preserveAspectRatio: 'none', class: 'mini-chart', 'aria-hidden': 'true' });
      const [x0,x1] = c.xDomain, [y0,y1] = c.yDomain;
      const sx = x => 2 + 140 * (x-x0)/(x1-x0);
      const sy = y => 50 - 48 * (y-y0)/(y1-y0);
      mini.append(svgEl('line', { x1: 2, x2: 142, y1: sy(0), y2: sy(0), stroke: '#666', 'stroke-dasharray': '3 3' }));
      for (const series of c.series.filter(s => visible.has(s.id))) {
        const d = series.points.map(([x,y],i) => (i ? 'L' : 'M') + sx(x).toFixed(2) + ',' + sy(y).toFixed(2)).join(' ');
        mini.append(svgEl('path', { d, fill: 'none', stroke: colors[series.id], 'stroke-width': 1.65, 'vector-effect': 'non-scaling-stroke' }));
      }
      btn.replaceChildren(el('span', '', shortTitles[c.id]), mini);
    });
  }

  function selectChart(id) {
    chart = selectedChart(id);
    const points = chart.series[0].points;
    slider.min = points[0][0];
    slider.max = points[points.length-1][0];
    $('chart-title').textContent = chart.title;
    $('chart-count').textContent = (data.charts.indexOf(chart)+1) + ' / ' + data.charts.length;
    $('chart-note').textContent = notes[id];
    renderThumbnails();
    draw();
  }

  function renderTable() {
    const table = el('table');
    const caption = el('caption', 'sr-only', 'Tipo efectivo de IRPF por salario bruto anual. Comparación del modelo actual, VOX y las alternativas TIPO A, B y C.');
    const head = el('thead');
    const headers = el('tr');
    const salaryHeader = el('th', '', 'Bruto anual');
    salaryHeader.scope = 'col';
    headers.append(salaryHeader);
    Object.keys(labels).forEach(id => {
      const th = el('th');
      th.scope = 'col';
      const span = el('span', 'series-label', labels[id]);
      span.style.setProperty('--series-color', colors[id]);
      th.append(span);
      headers.append(th);
    });
    head.append(headers);
    const body = el('tbody');
    data.table.forEach(row => {
      const tr = el('tr');
      const th = el('th', '', money(row.salary));
      th.scope = 'row';
      tr.append(th);
      Object.keys(labels).forEach(id => {
        const td = el('td', '', percent(row[id]));
        td.dataset.scenario = id;
        tr.append(td);
      });
      body.append(tr);
    });
    table.append(caption, head, body);
    $('comparison-table').replaceChildren(table);
  }

  for (const c of data.charts) {
    const btn = el('button', 'chart-option');
    btn.type = 'button';
    btn.id = 'pick-' + c.id;
    btn.setAttribute('aria-label', c.title);
    btn.setAttribute('aria-controls', 'main');
    btn.addEventListener('click', () => selectChart(c.id));
    $('chart-options').append(btn);
  }
  for (const scenario of data.scenarios) {
    const button = el('button', 'scenario');
    button.type = 'button';
    button.dataset.scenario = scenario.id;
    button.setAttribute('aria-pressed', 'true');
    const swatch = el('span', 'swatch');
    swatch.setAttribute('aria-hidden','true');
    button.append(swatch, el('span','',scenario.label));
    $('scenarios').append(button);
    const id = button.dataset.scenario;
    button.style.setProperty('--series-color', colors[id]);
    button.addEventListener('click', () => {
      if (visible.has(id) && visible.size === 1) {
        $('series-status').textContent = 'Mantén al menos un modelo seleccionado.';
        return;
      }
      visible.has(id) ? visible.delete(id) : visible.add(id);
      button.setAttribute('aria-pressed', String(visible.has(id)));
      $('series-status').textContent = `${visible.size} seleccionados`;
      renderThumbnails();
      draw();
    });
  }
  $('reset-scenarios').addEventListener('click', () => {
    Object.keys(labels).forEach(id => visible.add(id));
    document.querySelectorAll('button[data-scenario]').forEach(button => button.setAttribute('aria-pressed', 'true'));
    $('series-status').textContent = '5 seleccionados';
    renderThumbnails();
    draw();
  });
  $('export-data').addEventListener('click', () => {
    const rows = [['Salario bruto anual (€)', ...Object.values(labels).map(label => `${label} (%)`)]];
    data.table.forEach(row => rows.push([String(row.salary), ...Object.keys(labels).map(id => number(row[id] * 100, 2))]));
    const csv = '\uFEFF' + rows.map(row => row.join(';')).join('\r\n');
    const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8;' }));
    const link = el('a');
    link.href = url;
    link.download = 'reforma-irpf-tipos-efectivos.csv';
    document.body.append(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  });
  renderTable();
  slider.addEventListener('input', () => inspect(Number(slider.value)));
  selectChart(chart.id);
  new ResizeObserver(draw).observe(plot);
})();
