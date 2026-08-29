// Byte-level building blocks mirroring the real wire formats.

export type SegKind =
  | 'magic'
  | 'version'
  | 'type'
  | 'int'
  | 'offset'
  | 'len'
  | 'str'
  | 'blob'
  | 'crc'
  | 'payload'
  | 'count';

export interface Seg {
  label: string;
  value: string;
  kind: SegKind;
  bytes: Uint8Array;
}

export function segsBytes(segs: Seg[]): Uint8Array {
  const total = segs.reduce((n, s) => n + s.bytes.length, 0);
  const out = new Uint8Array(total);
  let at = 0;
  for (const s of segs) {
    out.set(s.bytes, at);
    at += s.bytes.length;
  }
  return out;
}

export function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join(' ');
}

export class SegWriter {
  segs: Seg[] = [];

  private push(label: string, kind: SegKind, bytes: Uint8Array, value: string) {
    this.segs.push({ label, kind, bytes, value });
  }

  u8(label: string, v: number, kind: SegKind = 'int') {
    this.push(label, kind, new Uint8Array([v & 0xff]), String(v));
  }

  u32le(label: string, v: number, kind: SegKind = 'int') {
    const b = new Uint8Array(4);
    new DataView(b.buffer).setUint32(0, v >>> 0, true);
    this.push(label, kind, b, String(v >>> 0));
  }

  i32le(label: string, v: number, kind: SegKind = 'int') {
    const b = new Uint8Array(4);
    new DataView(b.buffer).setInt32(0, v | 0, true);
    this.push(label, kind, b, String(v | 0));
  }

  u64le(label: string, v: number | bigint, kind: SegKind = 'int') {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigUint64(0, BigInt(v), true);
    this.push(label, kind, b, String(v));
  }

  i64le(label: string, v: number | bigint, kind: SegKind = 'int') {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigInt64(0, BigInt(v), true);
    this.push(label, kind, b, String(v));
  }

  u32be(label: string, v: number, kind: SegKind = 'int') {
    const b = new Uint8Array(4);
    new DataView(b.buffer).setUint32(0, v >>> 0, false);
    this.push(label, kind, b, String(v >>> 0));
  }

  i32be(label: string, v: number, kind: SegKind = 'int') {
    const b = new Uint8Array(4);
    new DataView(b.buffer).setInt32(0, v | 0, false);
    this.push(label, kind, b, String(v | 0));
  }

  u64be(label: string, v: number | bigint, kind: SegKind = 'int') {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigUint64(0, BigInt(v), false);
    this.push(label, kind, b, String(v));
  }

  /// u32-LE length prefix + UTF-8 bytes (metadata codec `put_str`).
  strLe(label: string, s: string) {
    const bytes = new TextEncoder().encode(s);
    this.u32le(`${label} len`, bytes.length, 'len');
    this.push(label, 'str', bytes, JSON.stringify(s));
  }

  blobLe(label: string, bytes: Uint8Array, display?: string) {
    this.u32le(`${label} len`, bytes.length, 'len');
    this.push(label, 'blob', bytes, display ?? `${bytes.length} bytes`);
  }

  raw(label: string, bytes: Uint8Array, kind: SegKind, value?: string) {
    this.push(label, kind, bytes, value ?? `${bytes.length} bytes`);
  }

  append(segs: Seg[]) {
    this.segs.push(...segs);
  }

  bytes(): Uint8Array {
    return segsBytes(this.segs);
  }
}

// CRC-32/ISO-HDLC (the standard zlib crc32).
const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[i] = c >>> 0;
  }
  return table;
})();

export function crc32(bytes: Uint8Array): number {
  let c = 0xffffffff;
  for (let i = 0; i < bytes.length; i++) {
    c = CRC_TABLE[(c ^ bytes[i]) & 0xff] ^ (c >>> 8);
  }
  return (c ^ 0xffffffff) >>> 0;
}

/// `WALUtil.crc32`: ISO-HDLC masked to 31 bits (s3stream-codec/src/crc.rs).
export function walCrc32(bytes: Uint8Array): number {
  return (crc32(bytes) & 0x7fffffff) >>> 0;
}

export function hexU32(v: number): string {
  return '0x' + (v >>> 0).toString(16).padStart(8, '0');
}

export function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MiB`;
}
