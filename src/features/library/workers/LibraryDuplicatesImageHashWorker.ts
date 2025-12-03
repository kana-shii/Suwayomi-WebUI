/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// eslint-disable-next-line no-restricted-globals
self.onmessage = async (
    ev: MessageEvent<{
        mangas: { id: string | number; thumbnailUrl?: string }[];
        threshold?: number;
        debug?: boolean;
    }>,
) => {
    const { mangas } = ev.data;

    // single source-of-truth default threshold (change here)
    const DEFAULT_DETECTION_THRESHOLD = 23;

    const DETECTION_THRESHOLD = typeof ev.data.threshold === 'number' ? ev.data.threshold : DEFAULT_DETECTION_THRESHOLD;
    const DEBUG = !!ev.data.debug;

    // backend base (from VITE_API_URL or fallback)
    const BACKEND_BASE =
        (typeof import.meta !== 'undefined' && (import.meta as any).env?.VITE_API_URL) || 'http://localhost:4567';

    function resolveUrl(url?: string | undefined): string | undefined {
        if (!url) return undefined;
        try {
            // If absolute, return as-is
            const parsed = new URL(url);
            return parsed.href;
        } catch {
            // Not absolute: prefix with BACKEND_BASE
            if (url.startsWith('/')) return `${BACKEND_BASE}${url}`;
            return `${BACKEND_BASE}/${url.replace(/^\/+/, '')}`;
        }
    }

    // DCT helper moved to top-level of handler (no-inner-declarations)
    const dct2 = (input: number[], n: number): number[] => {
        const output = new Array(n * n).fill(0);
        const c = (u: number) => (u === 0 ? 1 / Math.sqrt(2) : 1);
        const factor = Math.PI / (2 * n);
        for (let u = 0; u < n; u += 1) {
            for (let v = 0; v < n; v += 1) {
                let sum = 0;
                for (let x = 0; x < n; x += 1) {
                    for (let y = 0; y < n; y += 1) {
                        sum +=
                            input[x * n + y] * Math.cos((2 * x + 1) * u * factor) * Math.cos((2 * y + 1) * v * factor);
                    }
                }
                output[u * n + v] = (2 / n) * c(u) * c(v) * sum;
            }
        }
        return output;
    };

    function hexToBitString(hex: string): string {
        const h = hex.padStart(16, '0').toLowerCase();
        let bits = '';
        for (let i = 0; i < h.length; i += 1) {
            const nibble = parseInt(h[i], 16);
            bits += nibble.toString(2).padStart(4, '0');
        }
        return bits;
    }

    function hammingDistanceHex(a: string, b: string): number {
        if (!a || !b) return 64;
        const ba = hexToBitString(a);
        const bb = hexToBitString(b);
        let dist = 0;
        const len = Math.min(ba.length, bb.length);
        for (let i = 0; i < len; i += 1) if (ba[i] !== bb[i]) dist += 1;
        dist += Math.abs(ba.length - bb.length);
        return dist;
    }

    type HashInfo = {
        id: string;
        aHash: string | null;
        pHash: string | null;
        index: number;
    };

    // We will fetch each image only once and compute both hashes from the same bitmap.
    // Also process multiple images concurrently (bounded).
    const CONCURRENCY = Math.max(2, (navigator.hardwareConcurrency ?? 4) - 1);

    async function fetchBlobAndBitmap(url?: string): Promise<ImageBitmap | null> {
        const resolved = resolveUrl(url);
        if (!resolved) return null;
        try {
            const res = await fetch(resolved, { mode: 'cors' });
            if (!res.ok) return null;
            const blob = await res.blob();
            try {
                const bitmap = await createImageBitmap(blob);
                return bitmap;
            } catch {
                // failed to create bitmap
                return null;
            }
        } catch {
            return null;
        }
    }

    // compute both hashes from a single bitmap
    function computeHashesFromBitmap(bitmap: ImageBitmap | null): { aHash: string | null; pHash: string | null } {
        if (!bitmap) return { aHash: null, pHash: null };

        // aHash: resize to 8x8 and compute average luminance bits
        try {
            const aSize = 8;
            const canvasA = new OffscreenCanvas(aSize, aSize);
            const ctxA = canvasA.getContext('2d');
            if (!ctxA) return { aHash: null, pHash: null };
            ctxA.drawImage(bitmap, 0, 0, aSize, aSize);
            const imageDataA = ctxA.getImageData(0, 0, aSize, aSize);
            const { data: dataA } = imageDataA;
            let sum = 0;
            const lum: number[] = new Array(aSize * aSize);
            for (let i = 0, j = 0; i < dataA.length; i += 4, j += 1) {
                const r = dataA[i];
                const g = dataA[i + 1];
                const b = dataA[i + 2];
                const l = 0.2126 * r + 0.7152 * g + 0.0722 * b;
                lum[j] = l;
                sum += l;
            }
            const avg = sum / lum.length;
            let bits = '';
            for (let i = 0; i < lum.length; i += 1) bits += lum[i] >= avg ? '1' : '0';
            const hexParts: string[] = [];
            for (let i = 0; i < bits.length; i += 4) {
                const chunk = bits.substring(i, i + 4);
                const val = parseInt(chunk, 2);
                hexParts.push(val.toString(16));
            }
            const aHash = hexParts.join('').padStart(16, '0');

            // pHash: draw to 32x32 then DCT -> top-left 8x8
            const size = 32;
            const canvasP = new OffscreenCanvas(size, size);
            const ctxP = canvasP.getContext('2d');
            if (!ctxP) return { aHash, pHash: null };
            ctxP.drawImage(bitmap, 0, 0, size, size);
            const imageDataP = ctxP.getImageData(0, 0, size, size);
            const { data } = imageDataP;
            const gray: number[] = new Array(size * size);
            for (let i = 0, j = 0; i < data.length; i += 4, j += 1) {
                const r = data[i];
                const g2 = data[i + 1];
                const b2 = data[i + 2];
                gray[j] = 0.299 * r + 0.587 * g2 + 0.114 * b2;
            }

            const dct = dct2(gray, size);

            const smallSize = 8;
            const vals: number[] = [];
            for (let u = 0; u < smallSize; u += 1) {
                for (let v = 0; v < smallSize; v += 1) {
                    vals.push(dct[u * size + v]);
                }
            }

            const coeffsForMedian = vals.slice(1);
            const sorted = coeffsForMedian.slice().sort((a, b) => a - b);
            const mid = Math.floor(sorted.length / 2);
            const median = sorted.length % 2 === 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;

            let bitsP = '';
            for (let i = 0; i < vals.length; i += 1) {
                bitsP += vals[i] > median ? '1' : '0';
            }

            const hexPartsP: string[] = [];
            for (let i = 0; i < bitsP.length; i += 4) {
                const chunk = bitsP.substring(i, i + 4);
                const val = parseInt(chunk, 2);
                hexPartsP.push(val.toString(16));
            }
            const pHash = hexPartsP.join('').padStart(16, '0');

            return { aHash, pHash };
        } finally {
            // nothing to cleanup here (bitmap closed by caller)
        }
    }

    // Build tasks and run them with limited concurrency
    const tasks: (() => Promise<HashInfo>)[] = mangas.map((m, idx) => {
        const idStr = String(m.id);
        return async () => {
            let bitmap: ImageBitmap | null = null;
            try {
                bitmap = await fetchBlobAndBitmap(m.thumbnailUrl);
                if (!bitmap) return { id: idStr, aHash: null, pHash: null, index: idx };
                const { aHash, pHash } = computeHashesFromBitmap(bitmap);
                return { id: idStr, aHash, pHash, index: idx };
            } catch {
                return { id: idStr, aHash: null, pHash: null, index: idx };
            } finally {
                try {
                    if (bitmap && (bitmap as any).close) (bitmap as any).close();
                } catch {
                    // ignore
                }
            }
        };
    });

    async function runWithConcurrency<T>(funcs: (() => Promise<T>)[], concurrency: number): Promise<T[]> {
        const results = new Array<T>(funcs.length);
        let i = 0;

        /* eslint-disable no-await-in-loop, no-constant-condition */
        const workers: Promise<void>[] = new Array(Math.min(concurrency, funcs.length)).fill(null).map(async () => {
            while (true) {
                // get next index atomically (simple increment)
                const idx = i;
                i += 1;
                if (idx >= funcs.length) break;
                try {
                    // awaiting inside the loop is intentional for a bounded worker pool
                    // eslint-disable-next-line no-await-in-loop
                    results[idx] = await funcs[idx]();
                } catch (e) {
                    // eslint-disable-next-line no-console
                    console.error('Error processing image hash task', e);
                    // @ts-expect-error allow undefined
                    results[idx] = undefined;
                }
            }
        });
        /* eslint-enable no-await-in-loop, no-constant-condition */

        await Promise.all(workers);
        return results;
    }

    const hashResults = await runWithConcurrency(tasks, CONCURRENCY);

    // optionally collect sample debug data
    const debugSamples: { idA: string; idB: string; aDist: number; pDist: number; avg: number }[] = [];
    const n = hashResults.length;
    const parent = new Array(n);
    for (let i = 0; i < n; i += 1) parent[i] = i;
    function find(x: number): number {
        if (parent[x] !== x) parent[x] = find(parent[x]);
        return parent[x];
    }
    function union(a: number, b: number): void {
        const ra = find(a);
        const rb = find(b);
        if (ra !== rb) parent[rb] = ra;
    }

    for (let i = 0; i < n; i += 1) {
        const hi = hashResults[i];
        const hiHas = !!(hi?.aHash || hi?.pHash);
        if (hiHas) {
            for (let j = i + 1; j < n; j += 1) {
                const hj = hashResults[j];
                const hjHas = !!(hj?.aHash || hj?.pHash);
                if (!hjHas) {
                    // skip pairs where j has no hash
                } else {
                    const aHashA = hi?.aHash ?? hi?.pHash ?? null;
                    const aHashB = hj?.aHash ?? hj?.pHash ?? null;
                    const pHashA = hi?.pHash ?? hi?.aHash ?? null;
                    const pHashB = hj?.pHash ?? hj?.aHash ?? null;

                    const aDist = aHashA && aHashB ? hammingDistanceHex(aHashA, aHashB) : 64;
                    const pDist = pHashA && pHashB ? hammingDistanceHex(pHashA, pHashB) : 64;
                    const avg = (aDist + pDist) / 2;

                    if (DEBUG && debugSamples.length < 200) {
                        debugSamples.push({ idA: hi!.id, idB: hj!.id, aDist, pDist, avg });
                    }

                    if (avg <= DETECTION_THRESHOLD) {
                        union(i, j);
                    }
                }
            }
        }
    }

    const groupsMap = new Map<number, string[]>();
    for (let i = 0; i < n; i += 1) {
        const root = find(i);
        const arr = groupsMap.get(root);
        const { id } = hashResults[i] ?? { id: String(i) };
        if (arr) arr.push(id);
        else groupsMap.set(root, [id]);
    }

    const result: Record<string, typeof mangas> = {};
    let groupIndex = 0;
    for (const ids of groupsMap.values()) {
        if (ids.length > 1) {
            const group: typeof mangas = [];
            for (let gi = 0; gi < ids.length; gi += 1) {
                const id = ids[gi];
                for (let mi = 0; mi < mangas.length; mi += 1) {
                    if (String(mangas[mi].id) === id) {
                        group.push(mangas[mi]);
                        break;
                    }
                }
            }
            if (group.length > 1) {
                result[`imagehash:group:${groupIndex}`] = group;
                groupIndex += 1;
            }
        }
    }

    if (DEBUG) {
        postMessage({ result, debugSamples, thresholdUsed: DETECTION_THRESHOLD });
    } else {
        postMessage(result);
    }
};
