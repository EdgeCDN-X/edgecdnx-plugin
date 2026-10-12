#!/usr/bin/env node
import { readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { basename, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const metrics = ['zones', 'records', 'achieved_qps', 'avg_latency_ms', 'max_latency_ms'];
const optionalCounts = ['locations', 'locations_per_endpoint', 'nodes_per_location', 'node_groups_per_location'];
const number = new Intl.NumberFormat('en-US', { maximumFractionDigits: 3 });
const fixed = (value, digits) => value.toLocaleString('en-US', {
  minimumFractionDigits: digits, maximumFractionDigits: digits,
});
const escape = (value) => String(value).replace(/[&<>"']/g, (character) => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;',
})[character]);

export function parseCSV(source) {
  const rows = [];
  let row = [];
  let field = '';
  let quoted = false;
  let closed = false;
  source = source.replace(/^\uFEFF/, '');
  const finishField = () => {
    row.push(field);
    field = '';
    closed = false;
  };
  const finishRow = () => {
    finishField();
    if (row.some((value) => value.trim() !== '')) rows.push(row);
    row = [];
  };
  for (let index = 0; index < source.length; index++) {
    const character = source[index];
    if (quoted) {
      if (character === '"') {
        if (source[index + 1] === '"') {
          field += '"';
          index++;
        } else {
          quoted = false;
          closed = true;
        }
      } else {
        field += character;
      }
    } else if (character === ',') {
      finishField();
    } else if (character === '\n' || character === '\r') {
      if (character === '\r' && source[index + 1] === '\n') index++;
      finishRow();
    } else if (character === '"' && field === '' && !closed) {
      quoted = true;
    } else {
      if (closed || character === '"') throw new Error('Malformed CSV quoting.');
      field += character;
    }
  }
  if (quoted) throw new Error('Unterminated CSV quoted field.');
  if (field !== '' || closed || row.length > 0) finishRow();
  if (rows.length < 2) throw new Error('CSV must contain a header and at least one data row.');
  const headers = rows.shift().map((value) => value.trim());
  if (new Set(headers).size !== headers.length) throw new Error('CSV contains duplicate column names.');
  for (const metric of metrics) {
    if (!headers.includes(metric)) throw new Error(`Missing required CSV column: ${metric}`);
  }
  return rows.map((values, index) => {
    if (values.length !== headers.length) throw new Error(`Data row ${index + 1}: column count does not match header.`);
    const result = Object.fromEntries(headers.map((header, position) => [header, values[position].trim()]));
    for (const metric of [...metrics, ...optionalCounts.filter((column) => headers.includes(column))]) {
      const value = Number(result[metric]);
      if (result[metric] === '' || !Number.isFinite(value) || value < 0 ||
          (metric === 'zones' && value === 0) ||
          (['zones', 'records', ...optionalCounts].includes(metric) && !Number.isSafeInteger(value))) {
        throw new Error(`Data row ${index + 1}: invalid ${metric}: ${JSON.stringify(result[metric])}`);
      }
      result[metric] = value;
    }
    if (result.max_latency_ms < result.avg_latency_ms) {
      throw new Error(`Data row ${index + 1}: maximum duration is less than average duration.`);
    }
    return result;
  }).sort((left, right) => left.zones - right.zones);
}

function scale(maximum) {
  if (maximum === 0) return { top: 1, ticks: [0, 0.25, 0.5, 0.75, 1] };
  const rawStep = maximum / 4;
  const magnitude = 10 ** Math.floor(Math.log10(rawStep));
  const step = [1, 2, 2.5, 5, 10].find((candidate) => candidate * magnitude >= rawStep) * magnitude;
  const count = Math.ceil(maximum / step);
  return { top: step * count, ticks: Array.from({ length: count + 1 }, (_, index) => index * step) };
}

export function renderSVG(rows, sourceName) {
  if (!rows.length) throw new Error('No measurements to render.');
  const sourceLabel = basename(sourceName).replace(/\.csv$/i, '').toLowerCase();
  const hasGeo = rows.some((row) => row.geo_attribute !== undefined || row.node_groups_per_location !== undefined) ||
    /(?:^|[-_ ])(?:geo|geolookup|geolocation)(?:$|[-_ ])/.test(sourceLabel);
  const hasECS = rows.some((row) => row.ecs_us_subnet !== undefined || row.ecs_eu_subnet !== undefined);
  const hasWRR = rows.some((row) => row.location_weights !== undefined) ||
    /(?:^|[-_ ])(?:wrr|weighted)(?:$|[-_ ])/.test(sourceLabel);
  const isSimple = /(?:^|[-_ ])simple(?:$|[-_ ])/.test(sourceLabel);
  const testName = hasGeo ? `GeoLookup${hasECS ? ' (EDNS0 ECS)' : ''}` :
    hasWRR ? 'Weighted Round Robin (WRR)' : isSimple ? 'Simple routing' : 'DNS benchmark';
  const minimumZone = Math.min(...rows.map((row) => row.zones));
  const maximumZone = Math.max(...rows.map((row) => row.zones));
  const singleScale = minimumZone === maximumZone;
  const low = singleScale ? Math.log10(minimumZone) - 0.5 : Math.floor(Math.log10(minimumZone));
  const high = singleScale ? Math.log10(maximumZone) + 0.5 : Math.ceil(Math.log10(maximumZone));
  const zoneTicks = [];
  if (singleScale) {
    zoneTicks.push(minimumZone);
  } else {
    const step = Math.max(1, Math.ceil((high - low) / 4));
    for (let power = low; power <= high; power += step) zoneTicks.push(10 ** power);
    if (Math.log10(zoneTicks.at(-1)) < high) zoneTicks.push(10 ** high);
  }
  const tableBottom = 520 + rows.length * 41;
  const text = (x, y, content, className = 'tick', anchor = 'start') =>
    `<text x="${x}" y="${y}" class="${className}" text-anchor="${anchor}">${escape(content)}</text>`;
  const path = (d, className, color = '') =>
    `<path d="${d}" class="${className}"${color ? ` stroke="${color}"` : ''}/>`;
  const uniform = (column, suffix) => {
    const values = [...new Set(rows.map((row) => row[column]))];
    return values.length === 1 && values[0] !== undefined && values[0] !== ''
      ? `${values[0]} ${suffix}` : '';
  };
  const conditions = [
    uniform('duration_s', 'seconds per run'),
    uniform('clients', 'clients'),
    uniform('qps_limit', 'offered QPS'),
    uniform('locations_per_endpoint', 'locations/endpoint'),
    uniform('location_weights', 'location weights'),
    uniform('nodes_per_location', rows.some((row) => row.node_groups_per_location !== undefined) ? 'nodes/group' : 'nodes/location'),
    uniform('node_groups_per_location', 'node groups/location'),
  ].filter(Boolean);
  const geoConditions = [
    uniform('geo_attribute', 'geo attribute'),
    uniform('geo_values', 'geo values'),
    uniform('ecs_us_subnet', 'US ECS block'),
    uniform('ecs_eu_subnet', 'EU ECS block'),
  ].filter(Boolean);
  const height = tableBottom + 88 + (geoConditions.length ? 25 : 0);
  const hasLocations = rows.every((row) => row.locations !== undefined);
  const description = rows.map((row) =>
    `${row.zones} zones, ${row.records} records${hasLocations ? `, ${row.locations} locations` : ''}: ${row.achieved_qps} QPS, ` +
    `${row.avg_latency_ms} ms average, ${row.max_latency_ms} ms maximum.`).join(' ');
  const pieces = [
    `<svg xmlns="http://www.w3.org/2000/svg" width="1320" height="${height}" viewBox="0 0 1320 ${height}" role="img" aria-labelledby="title description">`,
    `<title id="title">${escape(testName)}: throughput and query duration by zone count</title>`,
    `<desc id="description">${escape(description)} Zone axes are logarithmic; vertical axes start at zero.</desc>`,
    `<style>
      text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #172b4d; }
      .heading { font-size: 28px; font-weight: 700; }
      .subtitle { font-size: 16px; fill: #52637a; }
      .panel-title { font-size: 19px; font-weight: 650; }
      .tick { font-size: 13px; fill: #52637a; }
      .label { font-size: 14px; font-weight: 600; }
      .grid { stroke: #e3e9f0; stroke-width: 1; fill: none; }
      .axis { stroke: #8c9bad; stroke-width: 1; fill: none; }
      .line { fill: none; stroke-width: 2.5; stroke-linejoin: round; }
      .table-heading { font-size: 14px; font-weight: 650; }
      .table-value { font-size: 16px; font-variant-numeric: tabular-nums; }
      .note { font-size: 14px; fill: #52637a; }
    </style>`,
    `<rect width="1320" height="${height}" fill="#f5f7fb"/>`,
    text(48, 54, `${testName}: scale vs. query performance`, 'heading'),
    text(48, 84, conditions.length ? conditions.slice(0, 3).join(' · ') : 'Measured throughput and query durations', 'subtitle'),
    ...(conditions.length > 3 ? [text(48, 105, conditions.slice(3).join(' · '), 'note')] : []),
  ];
  const panels = [
    { metric: 'achieved_qps', title: 'Achieved QPS', color: '#2166c1', digits: 2 },
    { metric: 'avg_latency_ms', title: 'Average query duration (ms)', color: '#008577', digits: 3 },
    { metric: 'max_latency_ms', title: 'Maximum query duration (ms)', color: '#b86418', digits: 3 },
  ];
  panels.forEach((panel, index) => {
    const offset = index * 426;
    const left = 100 + offset;
    const right = left + 300;
    const vertical = scale(Math.max(...rows.map((row) => row[panel.metric])));
    const x = (zone) => left + (Math.log10(zone) - low) / (high - low) * 300;
    const y = (value) => 380 - value / vertical.top * 200;
    pieces.push(`<rect x="${32 + offset}" y="112" width="404" height="334" rx="12" fill="white"/>`);
    pieces.push(text(56 + offset, 147, panel.title, 'panel-title'));
    for (const tick of vertical.ticks) {
      pieces.push(path(`M${left} ${y(tick)}H${right}`, 'grid'));
      pieces.push(text(left - 10, y(tick) + 5, number.format(tick), 'tick', 'end'));
    }
    for (const zone of zoneTicks) {
      pieces.push(path(`M${x(zone)} 180V380`, 'grid'));
      pieces.push(text(x(zone), 404, number.format(zone), 'tick', 'middle'));
    }
    pieces.push(path(`M${left} 180V380H${right}`, 'axis'));
    pieces.push(text(left + 150, 429, 'Number of zones (log scale)', 'tick', 'middle'));
    pieces.push(path(rows.map((row, position) =>
      `${position === 0 ? 'M' : 'L'}${x(row.zones)} ${y(row[panel.metric])}`).join(''), 'line', panel.color));
    rows.forEach((row) => {
      const px = x(row.zones);
      const py = y(row[panel.metric]);
      const label = panel.metric === 'achieved_qps' && row[panel.metric] >= 1000
        ? `${fixed(row[panel.metric] / 1000, 1)}k` : fixed(row[panel.metric], panel.digits);
      pieces.push(`<circle cx="${px}" cy="${py}" r="5" fill="${panel.color}" stroke="white" stroke-width="2"><title>${escape(descriptionFor(row, panel))}</title></circle>`);
      if (rows.length <= 6) {
        pieces.push(text(Math.max(left + 32, Math.min(right - 20, px)), py - 14, label, 'label', 'middle'));
      }
    });
  });
  pieces.push(`<rect x="32" y="468" width="1256" height="${tableBottom - 468}" rx="12" fill="white"/>`);
  const columns = hasLocations ? [
    [64, 'Zones', 'zones', 0, 'start'],
    [250, 'Records', 'records', 0, 'end'],
    [430, 'Locations', 'locations', 0, 'end'],
    [670, 'Achieved QPS', 'achieved_qps', 2, 'end'],
    [950, 'Average duration (ms)', 'avg_latency_ms', 3, 'end'],
    [1256, 'Maximum duration (ms)', 'max_latency_ms', 3, 'end'],
  ] : [
    [64, 'Zones', 'zones', 0, 'start'],
    [280, 'Records', 'records', 0, 'end'],
    [570, 'Achieved QPS (queries/s)', 'achieved_qps', 2, 'end'],
    [910, 'Average query duration (ms)', 'avg_latency_ms', 3, 'end'],
    [1256, 'Maximum query duration (ms)', 'max_latency_ms', 3, 'end'],
  ];
  columns.forEach(([x, title, , , anchor]) => pieces.push(text(x, 498, title, 'table-heading', anchor)));
  rows.forEach((row, index) => {
    pieces.push(path(`M48 ${515 + index * 41}H1272`, 'grid'));
    columns.forEach(([x, , metric, digits, anchor]) =>
      pieces.push(text(x, 542 + index * 41, fixed(row[metric], digits), 'table-value', anchor)));
  });
  pieces.push(text(48, tableBottom + 29, 'Points are measured runs; connecting lines are visual guides. Maxima are observed worst cases, not percentiles.', 'note'));
  pieces.push(text(48, tableBottom + 54, `Source: ${sourceName} · Exact values are shown in the table; no uncertainty estimates are inferred.`, 'note'));
  if (geoConditions.length) pieces.push(text(48, tableBottom + 79, geoConditions.join(' · '), 'note'));
  pieces.push('</svg>');
  return pieces.join('\n') + '\n';
}

function descriptionFor(row, panel) {
  return `${number.format(row.zones)} zones / ${number.format(row.records)} records` +
    `${row.locations !== undefined ? ` / ${number.format(row.locations)} locations` : ''}: ${panel.title} ${row[panel.metric]}`;
}

function main() {
  const { values, positionals } = parseArgs({
    options: {
      input: { type: 'string', short: 'i' },
      output: { type: 'string', short: 'o' },
      help: { type: 'boolean', short: 'h' },
    },
    allowPositionals: true,
  });
  if (values.help) {
    console.log(`Usage: node render-baseline.mjs [INPUT.csv | -i INPUT.csv] [-o OUTPUT.svg]

Render a standalone SVG with three log-zone charts and an exact-value table.
Required columns: ${metrics.join(', ')}
Supports Simple, WRR and GeoLookup CSVs, including per-endpoint locations.
Optional columns: duration_s, clients, qps_limit, locations, locations_per_endpoint,
                  location_weights, nodes_per_location, node_groups_per_location,
                  geo_attribute, geo_values, ecs_us_subnet, ecs_eu_subnet
Rows are sorted by zone count. Durations are in milliseconds.
Default output: INPUT's basename with .svg extension in the current directory.
Without input/output arguments, convert every CSV in the current directory to SVG.
Batch conversion continues after invalid files and exits with an error if any fail.
No dependencies are required. Existing output files are overwritten.`);
    return;
  }
  if (positionals.length > 1 || (values.input && positionals.length)) {
    throw new Error('Provide at most one input CSV, either positional or --input. Use --help for usage.');
  }
  const requestedInput = values.input ?? positionals[0];
  if (!requestedInput && values.output !== undefined) {
    throw new Error('--output requires an input CSV; omit both for batch conversion.');
  }
  const inputs = requestedInput !== undefined ? [resolve(requestedInput)] :
    readdirSync(process.cwd(), { withFileTypes: true })
      .filter((entry) => entry.isFile() && /\.csv$/i.test(entry.name))
      .map((entry) => resolve(entry.name)).sort();
  if (!inputs.length) throw new Error('No CSV files found in the current directory.');
  const failures = [];
  for (const input of inputs) {
    try {
      const output = resolve(values.output ?? basename(input).replace(/(?:\.csv)?$/i, '.svg'));
      if (input === output || inputs.includes(output)) {
        throw new Error('Output path must not overwrite an input CSV.');
      }
      const rows = parseCSV(readFileSync(input, 'utf8'));
      writeFileSync(output, renderSVG(rows, basename(input)), 'utf8');
      console.log(`Rendered ${rows.length} measurements to ${output}`);
    } catch (error) {
      failures.push(`${basename(input)}: ${error.message}`);
    }
  }
  if (failures.length) throw new Error(failures.join('\n'));
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    main();
  } catch (error) {
    console.error(`Error: ${error.message}`);
    process.exitCode = 1;
  }
}
