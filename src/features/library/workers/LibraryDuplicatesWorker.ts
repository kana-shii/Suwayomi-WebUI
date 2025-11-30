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

// Disjoint-set (union-find) implementation (unchanged)
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

function mergeDuplicateMapsAsComponents(
    mangas: TMangaDuplicate[],
    maps: TMangaDuplicates<TMangaDuplicate>[],
): TMangaDuplicates<TMangaDuplicate> {
    const idToIndex = new Map<string, number>();
    mangas.forEach((m, idx) => idToIndex.set(String(m.id), idx));
    const n = mangas.length;
    const uf = new UnionFind(n);

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
            if (idxs.length > 1) {
                const base = idxs[0];
                for (let j = 1; j < idxs.length; j += 1) {
                    uf.union(base, idxs[j]);
                }
            }
        }
    }

    const rootToMembers = new Map<number, number[]>();
    for (let i = 0; i < n; i += 1) {
        const root = uf.find(i);
        const arr = rootToMembers.get(root);
        if (arr) arr.push(i);
        else rootToMembers.set(root, [i]);
    }

    const result: TMangaDuplicates<TMangaDuplicate> = {};
    const roots = Array.from(rootToMembers.keys()).sort((a, b) => a - b);
    for (let ri = 0; ri < roots.length; ri += 1) {
        const root = roots[ri];
        const members = rootToMembers.get(root)!;
        if (members.length <= 1) {
            // skip singletons
        } else {
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
    const { mangas, checkAlternativeTitles, checkTrackedBySameTracker, checkImageHashes } = event.data;

    // title-only
    const onlyTitle = !checkAlternativeTitles && !checkTrackedBySameTracker && !checkImageHashes;
    // exclusive modes for each check
    const onlyTracker = checkTrackedBySameTracker && !checkAlternativeTitles && !checkImageHashes;
    const onlyDescription = checkAlternativeTitles && !checkTrackedBySameTracker && !checkImageHashes;
    const onlyImage = checkImageHashes && !checkTrackedBySameTracker && !checkAlternativeTitles;

    if (onlyTracker) {
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

    if (onlyDescription) {
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

    if (onlyImage) {
        const workerPromise = new ControlledPromise<TMangaDuplicates<TMangaDuplicate>>();
        const imageWorker = new Worker(new URL('LibraryDuplicatesImageHashWorker.ts', import.meta.url), {
            type: 'module',
        });
        imageWorker.onmessage = (ev: MessageEvent<TMangaDuplicates<TMangaDuplicate>>) => workerPromise.resolve(ev.data);
        // pass minimal mangas for hashing
        imageWorker.postMessage({ mangas } as { mangas: TMangaDuplicate[] });
        const imageResult = await workerPromise.promise;
        imageWorker.terminate();
        postMessage(imageResult);
        return;
    }

    if (onlyTitle) {
        const titleResult = findDuplicatesByTitle(mangas);
        postMessage(titleResult);
        return;
    }

    // otherwise multiple toggles enabled: compute all enabled checks in parallel and merge
    const resultsToMerge: TMangaDuplicates<TMangaDuplicate>[] = [];

    // title always cheap -> run synchronously
    resultsToMerge.push(findDuplicatesByTitle(mangas));

    // description (chunked) if enabled
    if (checkAlternativeTitles) {
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
        resultsToMerge.push(mergedTitleResult);
    }

    // tracker if enabled
    let trackerWorker: Worker | null = null;
    if (checkTrackedBySameTracker) {
        const wp = new ControlledPromise<TMangaDuplicates<TMangaDuplicate>>();
        trackerWorker = new Worker(new URL('LibraryDuplicatesTrackerWorker.ts', import.meta.url), {
            type: 'module',
        });
        trackerWorker.onmessage = (ev: MessageEvent<TMangaDuplicates<TMangaDuplicate>>) => wp.resolve(ev.data);
        trackerWorker.postMessage({ mangas } as { mangas: TMangaDuplicate[] });
        const trackerResult = await wp.promise;
        resultsToMerge.push(trackerResult);
    }

    // image hashes if enabled
    if (checkImageHashes) {
        const wp = new ControlledPromise<TMangaDuplicates<TMangaDuplicate>>();
        const imageWorker = new Worker(new URL('LibraryDuplicatesImageHashWorker.ts', import.meta.url), {
            type: 'module',
        });
        imageWorker.onmessage = (ev: MessageEvent<TMangaDuplicates<TMangaDuplicate>>) => wp.resolve(ev.data);
        imageWorker.postMessage({ mangas } as { mangas: TMangaDuplicate[] });
        const imageResult = await wp.promise;
        imageWorker.terminate();
        resultsToMerge.push(imageResult);
    }

    if (trackerWorker) trackerWorker.terminate();

    const merged = mergeDuplicateMapsAsComponents(mangas, resultsToMerge);
    postMessage(merged);
};
