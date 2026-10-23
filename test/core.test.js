/**
 * Tests for PaginatedStreamReader.
 *
 * Determinism: every test injects a synthetic in-memory source, so no real
 * file or network I/O occurs. No sleeps, no wall-clock comparisons.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { PaginatedStreamReader } from '../src/index.js';

/**
 * Build a synchronous-looking async range source backed by a Uint8Array.
 * Returns the requested range; clamps to the array boundary.
 */
function makeArraySource(bytes) {
  const buf = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  return async function source(offset, length) {
    if (offset < 0) throw new RangeError('negative offset');
    if (offset >= buf.length) return new Uint8Array(0);
    const end = Math.min(offset + length, buf.length);
    return buf.slice(offset, end);
  };
}

/** A source that rejects the first N calls then behaves normally. */
function makeFlakySource(bytes, rejectTimes) {
  const base = makeArraySource(bytes);
  let calls = 0;
  return async function source(offset, length) {
    calls += 1;
    if (calls <= rejectTimes) {
      throw new Error('transient source failure ' + calls);
    }
    return base(offset, length);
  };
}

describe('PaginatedStreamReader construction', () => {
  it('rejects a non-function source', () => {
    assert.throws(() => new PaginatedStreamReader(null, { pageSize: 4 }), TypeError);
  });

  it('rejects non-integer pageSize', () => {
    const src = makeArraySource(new Uint8Array(0));
    assert.throws(() => new PaginatedStreamReader(src, { pageSize: 4.5 }), RangeError);
  });

  it('rejects non-positive pageSize', () => {
    const src = makeArraySource(new Uint8Array(0));
    assert.throws(() => new PaginatedStreamReader(src, { pageSize: 0 }), RangeError);
    assert.throws(() => new PaginatedStreamReader(src, { pageSize: -1 }), RangeError);
  });

  it('rejects missing options', () => {
    const src = makeArraySource(new Uint8Array(0));
    assert.throws(() => new PaginatedStreamReader(src), RangeError);
  });
});

describe('PaginatedStreamReader.read', () => {
  it('returns a full page when enough bytes remain', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5, 6]);
    const r = new PaginatedStreamReader(makeArraySource(data), { pageSize: 4 });
    const page = await r.read();
    assert.equal(page.offset, 0);
    assert.equal(page.length, 4);
    assert.equal(page.pageIndex, 0);
    assert.equal(page.eof, false);
    assert.deepEqual(Array.from(page.data), [1, 2, 3, 4]);
  });

  it('returns a short final page and signals EOF', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5, 6]);
    const r = new PaginatedStreamReader(makeArraySource(data), { pageSize: 4 });
    await r.read();
    const page = await r.read();
    assert.equal(page.offset, 4);
    assert.equal(page.length, 2);
    assert.equal(page.pageIndex, 1);
    assert.equal(page.eof, true);
    assert.deepEqual(Array.from(page.data), [5, 6]);
  });

  it('returns an empty EOF page for an empty source', async () => {
    const r = new PaginatedStreamReader(makeArraySource(new Uint8Array(0)), { pageSize: 4 });
    const page = await r.read();
    assert.equal(page.length, 0);
    assert.equal(page.eof, true);
    assert.equal(page.data.length, 0);
  });

  it('keeps returning EOF page after EOF without touching the source', async () => {
    let sourceCalls = 0;
    const src = async () => { sourceCalls++; return new Uint8Array(0); };
    const r = new PaginatedStreamReader(src, { pageSize: 4 });
    const first = await r.read();
    assert.equal(first.eof, true);
    assert.equal(sourceCalls, 1);
    const second = await r.read();
    assert.equal(second.eof, true);
    assert.equal(second.length, 0);
    assert.equal(sourceCalls, 1); // no extra source call
  });

  it('errors when source returns more than pageSize', async () => {
    const src = async () => new Uint8Array(10);
    const r = new PaginatedStreamReader(src, { pageSize: 4 });
    await assert.rejects(() => r.read(), RangeError);
  });

  it('errors when source returns a non-Uint8Array', async () => {
    const src = async () => [];
    const r = new PaginatedStreamReader(src, { pageSize: 4 });
    await assert.rejects(() => r.read(), TypeError);
  });

  it('propagates transient source errors', async () => {
    const data = new Uint8Array([1, 2, 3, 4]);
    const r = new PaginatedStreamReader(makeFlakySource(data, 2), { pageSize: 4 });
    await assert.rejects(() => r.read(), /transient source failure 1/);
    await assert.rejects(() => r.read(), /transient source failure 2/);
    const page = await r.read();
    assert.equal(page.length, 4);
    assert.equal(page.eof, false);
    const eofPage = await r.read();
    assert.equal(eofPage.length, 0);
    assert.equal(eofPage.eof, true);
  });
});

describe('PaginatedStreamReader.readMany', () => {
  it('reads up to count pages stopping at EOF', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5, 6]);
    const r = new PaginatedStreamReader(makeArraySource(data), { pageSize: 2 });
    const pages = await r.readMany(10);
    assert.equal(pages.length, 4);
    assert.equal(pages[3].eof, true);
  });

  it('stops at count even if not at EOF', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
    const r = new PaginatedStreamReader(makeArraySource(data), { pageSize: 2 });
    const pages = await r.readMany(2);
    assert.equal(pages.length, 2);
    assert.equal(pages[1].eof, false);
    assert.equal(r.pageIndex, 2);
  });

  it('rejects negative count', async () => {
    const r = new PaginatedStreamReader(makeArraySource(new Uint8Array(0)), { pageSize: 2 });
    await assert.rejects(() => r.readMany(-1), RangeError);
  });

  it('returns empty array when already at EOF', async () => {
    const r = new PaginatedStreamReader(makeArraySource(new Uint8Array(0)), { pageSize: 2 });
    await r.read();
    const pages = await r.readMany(5);
    assert.equal(pages.length, 0);
  });
});

describe('PaginatedStreamReader.readAll', () => {
  it('collects every page until EOF', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    const r = new PaginatedStreamReader(makeArraySource(data), { pageSize: 2 });
    const pages = await r.readAll();
    assert.equal(pages.length, 3);
    assert.deepEqual(Array.from(pages[0].data), [1, 2]);
    assert.deepEqual(Array.from(pages[1].data), [3, 4]);
    assert.deepEqual(Array.from(pages[2].data), [5]);
    assert.equal(pages[2].eof, true);
  });
});

describe('PaginatedStreamReader.seek', () => {
  it('moves the offset and resets state', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5, 6]);
    const r = new PaginatedStreamReader(makeArraySource(data), { pageSize: 2 });
    await r.read();
    r.seek(4);
    assert.equal(r.offset, 4);
    assert.equal(r.pageIndex, 0);
    assert.equal(r.eof, false);
    const page = await r.read();
    assert.equal(page.offset, 4);
    assert.deepEqual(Array.from(page.data), [5, 6]);
  });

  it('rejects negative or non-integer offsets', () => {
    const r = new PaginatedStreamReader(makeArraySource(new Uint8Array(0)), { pageSize: 2 });
    assert.throws(() => r.seek(-1), RangeError);
    assert.throws(() => r.seek(1.5), RangeError);
  });

  it('clears EOF flag after seeking', async () => {
    const data = new Uint8Array([1, 2]);
    const r = new PaginatedStreamReader(makeArraySource(data), { pageSize: 2 });
    const first = await r.read();
    assert.equal(first.eof, false);
    const second = await r.read();
    assert.equal(second.eof, true);
    r.seek(0);
    assert.equal(r.eof, false);
    const again = await r.read();
    assert.equal(again.eof, false);
    assert.deepEqual(Array.from(again.data), [1, 2]);
    const eofAgain = await r.read();
    assert.equal(eofAgain.eof, true);
  });
});
