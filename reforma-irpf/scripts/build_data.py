#!/usr/bin/env python3
"""Extract the Reforma sheet and its embedded chart caches using stdlib only.

Usage: python3 build_data.py INPUT.xlsx OUTPUT.js
Does not open, recalculate, edit, or read data cells from other worksheets.
"""

import argparse
import hashlib
import json
import math
import posixpath
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile

NS = {
    's': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
    'c': 'http://schemas.openxmlformats.org/drawingml/2006/chart',
    'a': 'http://schemas.openxmlformats.org/drawingml/2006/main',
    'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
    'p': 'http://schemas.openxmlformats.org/package/2006/relationships',
}


def relationships(archive, part):
    folder, filename = posixpath.split(part)
    relpath = posixpath.join(folder, '_rels', filename + '.rels')
    root = ET.fromstring(archive.read(relpath))
    return {
        rel.get('Id'): (rel.get('Type'),
            rel.get('Target').lstrip('/') if rel.get('Target').startswith('/')
            else posixpath.normpath(posixpath.join(folder, rel.get('Target'))))
        for rel in root
    }


def numeric(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('Non-finite numeric source value')
    return int(result) if result.is_integer() else result


def cached_points(series, name):
    node = series.find('c:' + name, NS)
    if node is None:
        raise ValueError('Missing chart value component ' + name)
    formula = node.find('.//c:f', NS)
    cache = node.find('.//c:numCache', NS)
    if cache is None:
        raise ValueError('Missing embedded chart cache for ' + name)
    count = int(cache.find('c:ptCount', NS).get('val'))
    points = {int(p.get('idx')): numeric(p.find('c:v', NS).text)
              for p in cache.findall('c:pt', NS)}
    if sorted(points) != list(range(count)):
        raise ValueError('Missing or misaligned embedded chart points')
    return formula.text if formula is not None else None, [points[i] for i in range(count)]


def axis_domain(axis):
    return [numeric(axis.find('c:scaling/c:' + end, NS).get('val'))
            for end in ['min', 'max']]


def extract(source):
    with zipfile.ZipFile(source) as archive:
        workbook = ET.fromstring(archive.read('xl/workbook.xml'))
        sheet_info = next(s for s in workbook.findall('s:sheets/s:sheet', NS)
                          if s.get('name').casefold() == 'reforma')
        sheet_path = relationships(archive, 'xl/workbook.xml')[sheet_info.get('{' + NS['r'] + '}id')][1]
        sheet = ET.fromstring(archive.read(sheet_path))
        shared = [''.join(si.itertext()) for si in ET.fromstring(archive.read('xl/sharedStrings.xml')).findall('s:si', NS)]
        values = {}
        for cell in sheet.findall('s:sheetData/s:row/s:c', NS):
            v = cell.find('s:v', NS)
            kind = cell.get('t', 'n')
            if kind == 'e':
                raise ValueError('Source formula error at Reforma!' + cell.get('r'))
            if kind == 'inlineStr':
                values[cell.get('r')] = ''.join(cell.find('s:is', NS).itertext())
            elif v is not None:
                values[cell.get('r')] = shared[int(v.text)] if kind == 's' else numeric(v.text) if kind == 'n' else v.text
            elif cell.find('s:f', NS) is not None:
                raise ValueError('Formula has no cached value at Reforma!' + cell.get('r'))

        def value(address):
            if address not in values:
                raise ValueError('Missing Reforma!' + address)
            return values[address]

        expected = {'A86':'Salario bruto','B86':'Tipo actual','C86':'Tipo VOX','E86':'Tipo A','F86':'Tipo B','G86':'Tipo C'}
        for address, label in expected.items():
            if value(address) != label:
                raise ValueError('Unexpected table structure at Reforma!' + address)

        result = {
            'source': {
                'file': source.name,
                'sheet': sheet_info.get('name'),
                'sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
                'tableRange': 'A86:C102,E86:G102',
                'profileCells': ['A4','A5','A24','A25','A114'],
                'summaryRanges': ['F71','F109:F112','H109:H112','J109:J112'],
                'method': 'Saved values of Reforma and caches of the four charts embedded in that sheet. No recalculation.',
                'precision': {'salary':'whole EUR','rate':'two decimal percentage points','revenue':'one decimal million EUR'},
            },
            'profile': {address:value(address) for address in ['A4','A5','A24','A25','A114']},
            'scenarios': [
                {'id':'actual','label':'Tipo actual','description':'IRPF actual web del modelo'},
                {'id':'vox','label':'Reforma de VOX','description':'IRPF VOX web del modelo'},
                {'id':'a','label':'TIPO A','description':'Moderada'},
                {'id':'b','label':'TIPO B','description':'Equilibrada'},
                {'id':'c','label':'TIPO C','description':'Más redistributiva'},
            ],
            'table': [dict({'salary':value(f'A{r}')}, **{sid:value(f'{col}{r}') for sid,col in [('actual','B'),('vox','C'),('a','E'),('b','F'),('c','G')]}) for r in range(87,103)],
            'summary': {'unit':'millionEUR','actual':value('F71'),'candidates':[
                {'id':sid,'revenue':value(col+'109'),'additional':value(col+'110'),'ratio':value(col+'111'),'check':value(col+'112')}
                for sid,col in [('a','F'),('b','H'),('c','J')]
            ]},
            'charts': [],
        }

        drawing_ref = sheet.find('s:drawing', NS)
        drawing_path = relationships(archive, sheet_path)[drawing_ref.get('{' + NS['r'] + '}id')][1]
        drawing = ET.fromstring(archive.read(drawing_path))
        drawing_rels = relationships(archive, drawing_path)
        chart_refs = drawing.findall('.//c:chart', NS)
        if len(chart_refs) != 4:
            raise ValueError('Expected exactly four embedded Reforma charts')
        labels = {'Actual web':'actual','VOX web':'vox','Reforma A':'a','Reforma B':'b','Reforma C':'c'}
        title_ids = {
            'Ahorro anual frente al IRPF actual (€)':'savings',
            'Variación del sueldo neto (%)':'net',
            'Tipo efectivo de IRPF (% del bruto)':'rate',
            'Ahorro como porcentaje del bruto':'share',
        }
        clipping = []
        for chart_ref in chart_refs:
            chart_path = drawing_rels[chart_ref.get('{' + NS['r'] + '}id')][1]
            chart = ET.fromstring(archive.read(chart_path)).find('c:chart', NS)
            title = ' '.join(t.text or '' for t in chart.findall('c:title//a:t', NS))
            chart_id = title_ids[title]
            axes = chart.findall('c:plotArea/c:valAx', NS)
            x_axis = next(a for a in axes if a.find('c:axPos',NS).get('val') in ['b','t'])
            y_axis = next(a for a in axes if a.find('c:axPos',NS).get('val') in ['l','r'])
            output = {'id':chart_id,'title':title,'xUnit':'thousandEUR','xDomain':axis_domain(x_axis),'yDomain':axis_domain(y_axis),'series':[]}
            for series in chart.findall('c:plotArea/c:scatterChart/c:ser', NS):
                label = series.find('c:tx/c:v', NS).text
                if label not in labels:
                    continue
                x_range,xs = cached_points(series,'xVal')
                y_range,ys = cached_points(series,'yVal')
                if len(xs) != len(ys):
                    raise ValueError('Mismatched chart axes lengths')
                points = list(map(list,zip(xs,ys)))
                for x,y in points:
                    if not output['xDomain'][0] <= x <= output['xDomain'][1] or not output['yDomain'][0] <= y <= output['yDomain'][1]:
                        clipping.append({'chart':chart_id,'series':labels[label],'point':[x,y]})
                output['series'].append({'id':labels[label],'sourceLabel':label,'xRange':x_range,'yRange':y_range,'points':points})
            expected_ids = {'actual','vox','a','b','c'} if chart_id == 'rate' else {'vox','a','b','c'}
            if {s['id'] for s in output['series']} != expected_ids:
                raise ValueError('Unexpected chart series for ' + title)
            result['charts'].append(output)

        rate_chart = next(c for c in result['charts'] if c['id']=='rate')
        for series in rate_chart['series']:
            points = dict(series['points'])
            for row in result['table']:
                salary = row['salary']/1000
                if salary in points and abs(points[salary]-row[series['id']]) > 1e-12:
                    raise ValueError('Chart cache/table mismatch at salary ' + str(row['salary']))
        result['source']['clippedPoints'] = clipping
        return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input',type=Path)
    parser.add_argument('output',type=Path)
    args=parser.parse_args()
    data=extract(args.input)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
    if args.output.suffix == '.js':
        payload = '// Source: Reforma worksheet and its four embedded chart caches.\nwindow.REFORMA_DATA = ' + payload + ';'
    args.output.write_text(payload + '\n', encoding='utf-8')
    print(f"Extracted Reforma: {len(data['table'])} salary rows, {len(data['charts'])} charts, {len(data['source']['clippedPoints'])} clipped points.")


if __name__=='__main__':
    main()
