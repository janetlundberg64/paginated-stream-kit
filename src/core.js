/**
 * PaginatedStreamReader
 *
 * Reads fixed-size pages from an underlying seekable byte source identified
 * by (offset, length) coordinates. The reader treats the source as an opaque
 * byte stream and never interprets content.
 *
 * Why pagination lives here instead of in the caller:
 *   Callers reading very large files in a request-per-page pattern end up
 *   repeating the same bookkeeping — boundary clamping, page index math, EOF
 *   signalling, and offset validation. Centralising it keeps the caller free
 *   of off-by-one errors and makes the page contract explicit.
 *
 * Why the source is injected instead of opened here:
 *   This library has zero dependencies and cannot know whether bytes come
 *   from a file, a database blob, or a remote range request. Callers pass in
 *   any function matching AsyncByteRangeSource; the reader does not care.
 */

/**
 * @typedef {(offset: number, length: number) => Promise<Uint8Array>} AsyncByteRangeSource
 *   Returns a Uint8Array of up to `length` bytes starting at `offset`.
 *   May return fewer bytes than requested when the source cannot satisfy
 *   the full range (for example, near end of file). Must return an empty
 *   array when the offset is at or beyond the source boundary.
 */

/**
 * @typedef {Object} PageResult
 * @property {Uint8Array} data     The bytes for this page (empty at EOF).
 * @property {number} offset       Absolute offset of the first byte in `data`.
 * @property {number} length       Number of bytes in `data`.
 * @property {number} pageIndex    Zero-based index of this page since the first read.
 * @property {boolean} eof         True when no more bytes remain.
 */

export class PaginatedStreamReader {
  /**
   * @param {AsyncByteRangeSource} source
   * @param {Object} options
   * @param {number} options.pageSize  Bytes per page. Must be a positive integer.
   */
  constructor(source, options) {
    if (typeof source !== 'function') {
      throw new TypeError('source must be a function(offset, length) => Promise<Uint8Array>');
    }
    const pageSize = options?.pageSize;
    if (!Number.isInteger(pageSize) || pageSize <= 0) {
      throw new RangeError('options.pageSize must be a positive integer');
    }
    this._source = source;
    this._pageSize = pageSize;
    this._offset = 0;
    this._pageIndex = 0;
    this._eof = false;
  }

  /** Current absolute offset of the reader. */
  get offset() { return this._offset; }

  /** Zero-based index of the page the next read() will produce. */
  get pageIndex() { return this._pageIndex; }

  /** True once the reader has reported EOF; further read() calls return an empty page. */
  get eof() { return this._eof; }

  /**
   * Read the next page.
   *
   * Returns an empty-byte page with eof=true once the source is exhausted.
   * After EOF, subsequent calls keep returning the same EOF page without
   * touching the source — callers that loop on read() until eof can call
   * once more without error or I/O.
   *
   * @returns {Promise<PageResult>}
   */
  async read() {
    if (this._eof) {
      return {
        data: new Uint8Array(0),
        offset: this._offset,
        length: 0,
        pageIndex: this._pageIndex,
        eof: true,
      };
    }

    const offset = this._offset;
    const data = await this._source(offset, this._pageSize);

    if (!(data instanceof Uint8Array)) {
      throw new TypeError('source must return a Uint8Array');
    }
    if (data.length > this._pageSize) {
      throw new RangeError('source returned more bytes than requested');
    }

    const pageIndex = this._pageIndex;

    if (data.length === 0) {
      this._eof = true;
      return {
        data,
        offset,
        length: 0,
        pageIndex,
        eof: true,
      };
    }

    this._offset += data.length;
    this._pageIndex += 1;

    const eof = data.length < this._pageSize;
    if (eof) {
      this._eof = true;
    }

    return { data, offset, length: data.length, pageIndex, eof };
  }

  /**
   * Read up to `count` pages in one call. Stops early on EOF.
   *
   * @param {number} count  Maximum number of pages to read.
   * @returns {Promise<PageResult[]>}
   */
  async readMany(count) {
    if (!Number.isInteger(count) || count < 0) {
      throw new RangeError('count must be a non-negative integer');
    }
    const results = [];
    for (let i = 0; i < count && !this._eof; i++) {
      results.push(await this.read());
    }
    return results;
  }

  /**
   * Read all remaining pages until EOF.
   *
   * Use with care on unbounded sources — the loop only ends when the source
   * returns an empty page or a short page.
   *
   * @returns {Promise<PageResult[]>}
   */
  async readAll() {
    const results = [];
    while (!this._eof) {
      results.push(await this.read());
    }
    return results;
  }

  /**
   * Move the reader to an absolute offset. Resets EOF and the page index.
   *
   * @param {number} offset  Non-negative integer byte offset.
   */
  seek(offset) {
    if (!Number.isInteger(offset) || offset < 0) {
      throw new RangeError('offset must be a non-negative integer');
    }
    this._offset = offset;
    this._pageIndex = 0;
    this._eof = false;
  }
}
