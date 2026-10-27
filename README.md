# paginated-stream-kit

A small, zero-dependency ESM library that reads fixed-size pages from any seekable byte source using offset and length coordinates.

## Usage

```js
import { PaginatedStreamReader } from 'paginated-stream-kit';

// Any async function matching (offset, length) => Promise<Uint8Array> works.
const source = async (offset, length) => {
  const buf = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
  if (offset >= buf.length) return new Uint8Array(0);
  return buf.slice(offset, Math.min(offset + length, buf.length));
};

const reader = new PaginatedStreamReader(source, { pageSize: 3 });

for (let page = await reader.read(); !page.eof; page = await reader.read()) {
  console.log(page.offset, page.length, Array.from(page.data));
}
// 0 3 [1, 2, 3]
// 3 3 [4, 5, 6]
// 6 2 [7, 8]
```

## Why this exists

Reading a large file one page at a time sounds trivial until you write the loop: you have to clamp the last page at the boundary, distinguish "short page because EOF" from "short page because the source felt like it", signal EOF to the caller, validate the offset, and keep the page index honest. Each of those is a one-line bug in the wild. This library does that bookkeeping once.

The trade-off: the library does not open files or fetch URLs. It takes an injected `source` function with the signature `(offset, length) => Promise<Uint8Array>`. That keeps the dependency list empty and lets the same reader work over a file handle, a database blob, or a remote range request. You write the adapter; the reader handles the pagination contract.

## Edge cases

- The final page may be shorter than `pageSize`. The reader treats a short page as EOF, and any subsequent `read()` returns an empty page without calling the source again.
- If the source ever returns more bytes than requested, the reader throws `RangeError` rather than silently advancing past the boundary.
- After EOF, `readAll` and `readMany` stop immediately; calling `read()` again is safe and cheap.
- `seek` resets the page index and clears EOF, so a reader can be reused for multiple passes over the same source.
- `pageSize` must be a positive integer. Fractional or zero values are rejected at construction.

## Exports

- `PaginatedStreamReader` — the only class. Constructed with `(source, { pageSize })`. Methods: `read()`, `readMany(count)`, `readAll()`, `seek(offset)`. Properties: `offset`, `pageIndex`, `eof`.

## Design notes

The window stores values eagerly rather than keeping running aggregates. Running
sums drift with floating point over long streams, and recomputing from a small
buffer is cheap enough that the drift is not worth the speed.

