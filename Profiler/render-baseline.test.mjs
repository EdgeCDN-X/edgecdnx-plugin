import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { parseCSV, renderSVG } from './render-baseline.mjs';

const header = 'zones,records,achieved_qps,avg_latency_ms,max_latency_ms';
const source = `${header}\n100,1486,63855.3,1.437,11.954\n10,151,61223.57,1.475,72.149\n`;

test('reads and sorts measurements without rounding them', () => {
  const rows = parseCSV(source);
  assert.equal(rows[0].zones, 10);
  assert.equal(rows[0].achieved_qps, 61223.57);
  assert.equal(rows[1].records, 1486);
});

test('supports BOM, CRLF, quoted commas, escaped quotes and multiline fields', () => {
  const rows = parseCSV(`\uFEFF${header},note\r\n10,151,61223.57,1.475,72.149,"a,b\r\n""quoted"""\r\n`);
  assert.equal(rows[0].note, 'a,b\r\n"quoted"');
});

test('rejects missing columns, malformed CSV and empty data', () => {
  for (const input of [
    'zones,records\n10,151', `${header}\n`, `${header}\n10,151,2,1`,
    `${header}\n10,151,2,1,"3`, `${header}\n10,151,2,1,"3"x`,
    `${header},zones\n10,151,2,1,3,10`,
  ]) assert.throws(() => parseCSV(input));
});

test('rejects invalid and inconsistent metrics', () => {
  for (const row of [
    '0,151,2,1,3', '10.5,151,2,1,3', '10,151,-2,1,3',
    '10,151,Infinity,1,3', '10,151,,1,3', '10,151,2,4,3',
    '10,1.5,2,1,3',
  ]) assert.throws(() => parseCSV(`${header}\n${row}`));
});

test('renders all five metrics, escapes source names and uses log axes', () => {
  const svg = renderSVG(parseCSV(source), '<baseline & results>.csv');
  for (const value of ['61,223.57', '1,486', '1.437', '72.149', 'Number of zones (log scale)']) {
    assert.ok(svg.includes(value), value);
  }
  assert.ok(svg.includes('&lt;baseline &amp; results&gt;.csv'));
  assert.ok(!svg.includes('NaN'));
});

test('single-zone and zero-valued measurements have finite coordinates', () => {
  const svg = renderSVG(parseCSV(`${header}\n1,0,0,0,0`), 'zero.csv');
  assert.ok(!/NaN|Infinity/.test(svg));
  assert.ok(svg.includes('cx="250"'));
});

test('grows the table for more measurements and suppresses crowded point labels', () => {
  const rows = parseCSV(`${header}\n${Array.from({ length: 10 }, (_, index) =>
    `${index + 1},15,100,1,2`).join('\n')}`);
  const svg = renderSVG(rows, 'many.csv');
  assert.ok(svg.includes('height="1018"'));
  assert.equal((svg.match(/class="table-value"/g) ?? []).length, 50);
  assert.ok(!svg.includes('class="label"'));
});

test('renders Simple, shared WRR and per-endpoint WRR profiler CSVs', () => {
  const fixtures = [
    ['simple.csv', source],
    ['wrr-per-endpoint.csv', `${header},locations,locations_per_endpoint,location_weights,nodes_per_location\n10,159,2458.79,40.512,533.839,1590,10,"1,2,3",1`],
    ['wrr-shared.csv', `${header},locations,location_weights,nodes_per_location\n10,15,100,1,2,3,"1,2,3",1`],
  ];
  for (const [file, csv] of fixtures) {
    const rows = parseCSV(csv);
    const svg = renderSVG(rows, file);
    assert.ok(!/NaN|Infinity/.test(svg));
    if (file.startsWith('wrr')) assert.ok(svg.includes('Locations'));
    if (file === 'wrr-per-endpoint.csv') {
      assert.ok(svg.includes('1,590'));
      assert.ok(svg.includes('10 locations/endpoint'));
      assert.ok(svg.includes('1,2,3 location weights'));
    }
  }
});

test('validates optional location counts', () => {
  for (const column of ['locations', 'locations_per_endpoint', 'nodes_per_location']) {
    for (const value of ['', '-1', '1.5', 'Infinity']) {
      assert.throws(() => parseCSV(`${header},${column}\n10,15,100,1,2,${value}`), /invalid/);
    }
  }
});

const script = fileURLToPath(new URL('./render-baseline.mjs', import.meta.url));
function cliTest(t, args = []) {
  const directory = mkdtempSync(join(tmpdir(), 'profiler-render-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const run = (options = args) => spawnSync(process.execPath, [script, ...options], {
    cwd: directory, encoding: 'utf8',
  });
  return { directory, run };
}

test('no arguments converts all CSVs in the current folder', (t) => {
  const { directory, run } = cliTest(t);
  writeFileSync(join(directory, 'simple.csv'), source);
  writeFileSync(join(directory, 'weighted.CSV'), `${header},locations\n10,15,100,1,2,150`);
  writeFileSync(join(directory, 'notes.txt'), 'not CSV');
  assert.equal(run().status, 0);
  assert.ok(readFileSync(join(directory, 'simple.svg'), 'utf8').startsWith('<svg'));
  assert.ok(readFileSync(join(directory, 'weighted.svg'), 'utf8').includes('Locations'));
  assert.equal(readdirSync(directory).filter((file) => file.endsWith('.svg')).length, 2);
});

test('single-file input and output arguments remain optional independently', (t) => {
  const { directory, run } = cliTest(t);
  writeFileSync(join(directory, 'input.csv'), source);
  assert.equal(run(['input.csv']).status, 0);
  assert.equal(run(['--input', 'input.csv', '--output', 'custom.svg']).status, 0);
  assert.ok(readFileSync(join(directory, 'custom.svg'), 'utf8').includes('input.csv'));
  assert.notEqual(run(['--output', 'custom.svg']).status, 0);
  assert.notEqual(run(['input.csv', '-i', 'input.csv']).status, 0);
  assert.notEqual(run(['input.csv', '-o', 'input.csv']).status, 0);
  assert.equal(readFileSync(join(directory, 'input.csv'), 'utf8'), source);
});

test('batch conversion reports invalid CSVs while converting valid files', (t) => {
  const { directory, run } = cliTest(t);
  writeFileSync(join(directory, 'bad.csv'), 'invalid');
  writeFileSync(join(directory, 'good.csv'), source);
  const result = run();
  assert.equal(result.status, 1);
  assert.match(result.stderr, /bad.csv/);
  assert.ok(readFileSync(join(directory, 'good.svg'), 'utf8').includes('good.csv'));
});

test('empty folders report no CSVs', (t) => {
  const { run } = cliTest(t);
  const result = run();
  assert.equal(result.status, 1);
  assert.match(result.stderr, /No CSV files/);
});
