/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import { findDuplicatesByTitle } from '@/features/library/util/LibraryDuplicates.util.ts';
import {
    LibraryDuplicatesDescriptionWorkerInput,
    LibraryDuplicatesWorkerInput,
    TMangaDuplicate,
    TMangaDuplicates,
} from '@/features/library/Library.types.ts';
import { Queue } from '@/lib/Queue.ts';
import { ControlledPromise } from '@/lib/ControlledPromise.ts';
import { enhancedCleanup } from '@/base/utils/Strings.ts';

const queue = new Queue((navigator.hardwareConcurrency ?? 5) - 1);
const MANGAS_PER_CHUNK = 200;

// Disjoint-set (union-find) for efficient merging of groups.
class UnionFind {
    parent: number[];

    constructor(n: number) {
        this.parent = new Array(n);
        for (let i = 0; i < n; i += 1) this.parent[i] = i;
    }

    find(a: number): number {
        let p = a;
        while (this.parent[p] !== p) {
            p = this.parent[p];
        }
        // path compression
        let cur = a;
        while (this.parent[cur] !== cur) {
            const next = this.parent[cur];
            this.parent[cur] = p;
            cur = next;
        }
        return p;
    }

    union(a: number, b: number) {
        const pa = this.find(a);
        const pb = this.find(b);
        if (pa === pb) return;
        this.parent[pb] = pa;
    }
}

// Merge multiple duplicate maps into connected components (transitive merge) using union-find
function mergeDuplicateMapsAsComponents(
    mangas: TMangaDuplicate[],
    maps: TMangaDuplicates<TMangaDuplicate>[],
): TMangaDuplicates<TMangaDuplicate> {
    const idToIndex = new Map<string, number>();
    mangas.forEach((m, idx) => idToIndex.set(String(m.id), idx));
    const n = mangas.length;
    const uf = new UnionFind(n);

    // For each group in each map, union all members with the first member (O(k) per group)
    for (let mi = 0; mi < maps.length; mi += 1) {
        const map = maps[mi];
        const groups = Object.values(map);
        for (let gi = 0; gi < groups.length; gi += 1) {
            const group = groups[gi];
            const idxs: number[] = [];
            for (let i = 0; i < group.length; i += 1) {
                const idx = idToIndex.get(String(group[i].id));
                if (idx !== undefined) idxs.push(idx);
            }
            if (idxs.length <= 1) {
                // nothing to union for this group
            } else {
                const base = idxs[0];
                for (let j = 1; j < idxs.length; j += 1) {
                    uf.union(base, idxs[j]);
                }
            }
        }
    }

    // gather components
    const rootToMembers = new Map<number, number[]>();
    for (let i = 0; i < n; i += 1) {
        const root = uf.find(i);
        const arr = rootToMembers.get(root);
        if (arr) arr.push(i);
        else rootToMembers.set(root, [i]);
    }

    const result: TMangaDuplicates<TMangaDuplicate> = {};
    // deterministic order: sort roots by smallest index
    const roots = Array.from(rootToMembers.keys()).sort((a, b) => a - b);
    for (let ri = 0; ri < roots.length; ri += 1) {
        const root = roots[ri];
        const members = rootToMembers.get(root)!;
        if (members.length <= 1) {
            // singletons are not duplicates
        } else {
            // build group in the original order of indices
            const group: TMangaDuplicate[] = [];
            for (let mi = 0; mi < members.length; mi += 1) {
                group.push(mangas[members[mi]]);
            }
            const key = group[0].title ?? `combined-${group.map((g) => g.id).join('-')}`;
            result[key] = group;
        }
    }

    return result;
}

// eslint-disable-next-line no-restricted-globals
self.onmessage = async (event: MessageEvent<LibraryDuplicatesWorkerInput>) => {
    const { mangas, checkAlternativeTitles, checkTrackedBySameTracker } = event.data;

    // 1) EXCLUSIVE: tracker-only (user enabled only tracker toggle)
    if (checkTrackedBySameTracker && !checkAlternativeTitles) {
        const workerPromise = new ControlledPromise<TMangaDuplicates<TMangaDuplicate>>();

        const trackerWorker = new Worker(new URL('LibraryDuplicatesTrackerWorker.ts', import.meta.url), {
            type: 'module',
        });

        trackerWorker.onmessage = (trackerEvent: MessageEvent<TMangaDuplicates<TMangaDuplicate>>) =>
            workerPromise.resolve(trackerEvent.data);

        trackerWorker.postMessage({ mangas } as { mangas: TMangaDuplicate[] });

        const trackerResult = await workerPromise.promise;
        trackerWorker.terminate();

        postMessage(trackerResult);
        return;
    }

    // 2) EXCLUSIVE: description-only (user enabled only description toggle)
    if (checkAlternativeTitles && !checkTrackedBySameTracker) {
        // chunked description worker path (unchanged behavior)
        const chunkPromises: Promise<TMangaDuplicates<TMangaDuplicate>>[] = [];
        for (let chunkStart = 0; chunkStart < mangas.length; chunkStart += MANGAS_PER_CHUNK) {
            chunkPromises.push(
                queue.enqueue(chunkStart.toString(), () => {
                    const workerPromise = new ControlledPromise<TMangaDuplicates<TMangaDuplicate>>();

                    const worker = new Worker(new URL('LibraryDuplicatesDescriptionWorker.ts', import.meta.url), {
                        type: 'module',
                    });

                    worker.onmessage = (subWorkerEvent: MessageEvent<TMangaDuplicates<TMangaDuplicate>>) =>
                        workerPromise.resolve(subWorkerEvent.data);

                    worker.postMessage({
                        mangas,
                        mangasToCheck: mangas.slice(chunkStart, chunkStart + MANGAS_PER_CHUNK),
                    } satisfies LibraryDuplicatesDescriptionWorkerInput);

                    return workerPromise.promise;
                }).promise,
            );
        }

        const chunkedResults = await Promise.all(chunkPromises);
        const mergedResult: TMangaDuplicates<TMangaDuplicate> = {};

        const cleanedUpTitleToOriginalTitle: Record<string, string> = {};
        for (let ci = 0; ci < chunkedResults.length; ci += 1) {
            const chunkedResult = chunkedResults[ci];
            const entries = Object.entries(chunkedResult);
            for (let ei = 0; ei < entries.length; ei += 1) {
                const [title, duplicates] = entries[ei];
                const cleanedTitle = enhancedCleanup(title);
                if (cleanedUpTitleToOriginalTitle[cleanedTitle] === undefined) {
                    cleanedUpTitleToOriginalTitle[cleanedTitle] = title;
                }
                const originalTitle = cleanedUpTitleToOriginalTitle[cleanedTitle];

                if (mergedResult[originalTitle] === undefined) {
                    mergedResult[originalTitle] = duplicates;
                }
            }
        }

        postMessage(mergedResult);
        return;
    }

    // 3) EXCLUSIVE: title-only (neither toggle enabled)
    if (!checkTrackedBySameTracker && !checkAlternativeTitles) {
        const titleResult = findDuplicatesByTitle(mangas);
        postMessage(titleResult);
        return;
    }

    // 4) BOTH toggles enabled -> compute both in parallel and merge (combined)
    // Prepare tracker worker
    const trackerPromiseCtrl = new ControlledPromise<TMangaDuplicates<TMangaDuplicate>>();
    const trackerWorker = new Worker(new URL('LibraryDuplicatesTrackerWorker.ts', import.meta.url), {
        type: 'module',
    });
    trackerWorker.onmessage = (trackerEvent: MessageEvent<TMangaDuplicates<TMangaDuplicate>>) =>
        trackerPromiseCtrl.resolve(trackerEvent.data);
    trackerWorker.postMessage({ mangas } as { mangas: TMangaDuplicate[] });

    // compute title/description-based result (chunked)
    const chunkPromises: Promise<TMangaDuplicates<TMangaDuplicate>>[] = [];
    for (let chunkStart = 0; chunkStart < mangas.length; chunkStart += MANGAS_PER_CHUNK) {
        chunkPromises.push(
            queue.enqueue(chunkStart.toString(), () => {
                const workerPromise = new ControlledPromise<TMangaDuplicates<TMangaDuplicate>>();

                const worker = new Worker(new URL('LibraryDuplicatesDescriptionWorker.ts', import.meta.url), {
                    type: 'module',
                });

                worker.onmessage = (subWorkerEvent: MessageEvent<TMangaDuplicates<TMangaDuplicate>>) =>
                    workerPromise.resolve(subWorkerEvent.data);

                worker.postMessage({
                    mangas,
                    mangasToCheck: mangas.slice(chunkStart, chunkStart + MANGAS_PER_CHUNK),
                } satisfies LibraryDuplicatesDescriptionWorkerInput);

                return workerPromise.promise;
            }).promise,
        );
    }

    const chunkedResults = await Promise.all(chunkPromises);
    const mergedTitleResult: TMangaDuplicates<TMangaDuplicate> = {};
    const cleanedUpTitleToOriginalTitle: Record<string, string> = {};
    for (let ci = 0; ci < chunkedResults.length; ci += 1) {
        const chunkedResult = chunkedResults[ci];
        const entries = Object.entries(chunkedResult);
        for (let ei = 0; ei < entries.length; ei += 1) {
            const [title, duplicates] = entries[ei];
            const cleanedTitle = enhancedCleanup(title);
            if (cleanedUpTitleToOriginalTitle[cleanedTitle] === undefined) {
                cleanedUpTitleToOriginalTitle[cleanedTitle] = title;
            }
            const originalTitle = cleanedUpTitleToOriginalTitle[cleanedTitle];

            if (mergedTitleResult[originalTitle] === undefined) {
                mergedTitleResult[originalTitle] = duplicates;
            }
        }
    }

    const trackerResult = await trackerPromiseCtrl.promise;
    trackerWorker.terminate();

    const merged = mergeDuplicateMapsAsComponents(mangas, [mergedTitleResult, trackerResult]);
    postMessage(merged);
};
