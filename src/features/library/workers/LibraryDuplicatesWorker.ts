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

// Merge multiple duplicate maps into connected components (transitive merge)
function mergeDuplicateMapsAsComponents(
    mangas: TMangaDuplicate[],
    maps: TMangaDuplicates<TMangaDuplicate>[],
): TMangaDuplicates<TMangaDuplicate> {
    const idToIndex = new Map<string | number, number>();
    mangas.forEach((m, idx) => idToIndex.set(String(m.id), idx));
    const n = mangas.length;
    const adj: Set<number>[] = Array.from({ length: n }, () => new Set<number>());

    function addGroupEdges(group: TMangaDuplicate[]) {
        const idxs = group.map((m) => idToIndex.get(String(m.id))).filter((v): v is number => v !== undefined);
        for (let i = 0; i < idxs.length; i++) {
            for (let j = i + 1; j < idxs.length; j++) {
                const a = idxs[i];
                const b = idxs[j];
                adj[a].add(b);
                adj[b].add(a);
            }
        }
    }

    maps.forEach((map) => {
        Object.values(map).forEach((group) => addGroupEdges(group));
    });

    const visited = new Array<boolean>(n).fill(false);
    const result: TMangaDuplicates<TMangaDuplicate> = {};

    for (let i = 0; i < n; i++) {
        // process only unvisited nodes
        if (!visited[i]) {
            // BFS to collect component
            const stack = [i];
            visited[i] = true;
            const comp: number[] = [];
            while (stack.length) {
                const cur = stack.pop()!;
                comp.push(cur);
                adj[cur].forEach((ne) => {
                    if (!visited[ne]) {
                        visited[ne] = true;
                        stack.push(ne);
                    }
                });
            }
            if (comp.length > 1) {
                const group = comp.map((idx) => mangas[idx]);
                // choose key: try first title, fallback to combined ids
                const key = group[0].title ?? `combined-${group.map((g) => g.id).join('-')}`;
                result[key] = group;
            }
        }
    }

    return result;
}

// eslint-disable-next-line no-restricted-globals
self.onmessage = async (event: MessageEvent<LibraryDuplicatesWorkerInput>) => {
    const { mangas, checkAlternativeTitles, checkTrackedBySameTracker } = event.data;

    // Fast path: tracker-only requested -> spawn the tracker-specific worker and return its result
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

    // compute title/description-based result
    let titleOrDescriptionResult: TMangaDuplicates<TMangaDuplicate> = {};
    if (!checkAlternativeTitles) {
        titleOrDescriptionResult = findDuplicatesByTitle(mangas);
    } else {
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
        chunkedResults.forEach((chunkedResult) =>
            Object.entries(chunkedResult).forEach(([title, duplicates]) => {
                const cleanedTitle = enhancedCleanup(title);
                cleanedUpTitleToOriginalTitle[cleanedTitle] ??= title;
                const originalTitle = cleanedUpTitleToOriginalTitle[cleanedTitle];

                // ignore duplicated results for a title from other chunked results
                mergedResult[originalTitle] ??= duplicates;
            }),
        );

        titleOrDescriptionResult = mergedResult;
    }

    // If tracker checking is enabled too, compute tracker groups via the dedicated tracker worker & merge results
    if (checkTrackedBySameTracker) {
        const workerPromise = new ControlledPromise<TMangaDuplicates<TMangaDuplicate>>();

        const trackerWorker = new Worker(new URL('LibraryDuplicatesTrackerWorker.ts', import.meta.url), {
            type: 'module',
        });

        trackerWorker.onmessage = (trackerEvent: MessageEvent<TMangaDuplicates<TMangaDuplicate>>) =>
            workerPromise.resolve(trackerEvent.data);

        trackerWorker.postMessage({ mangas } as { mangas: TMangaDuplicate[] });

        const trackerResult = await workerPromise.promise;
        trackerWorker.terminate();

        const merged = mergeDuplicateMapsAsComponents(mangas, [titleOrDescriptionResult, trackerResult]);
        postMessage(merged);
        return;
    }

    // default (title/description only)
    postMessage(titleOrDescriptionResult);
};
