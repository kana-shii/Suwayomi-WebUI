/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import { TMangaDuplicate, TMangaDuplicates } from '@/features/library/Library.types.ts';

// eslint-disable-next-line no-restricted-globals
self.onmessage = (event: MessageEvent<{ mangas: TMangaDuplicate[] }>) => {
    const { mangas } = event.data;

    const map: Record<string, TMangaDuplicate[]> = {};

    mangas.forEach((m) => {
        const nodes = m.trackRecords?.nodes ?? [];
        nodes.forEach((tr) => {
            // only valid if remoteId exists; otherwise tracker binding cannot be used
            if (!tr.remoteId) return;
            const key = `${tr.trackerId}::${tr.remoteId}`;
            map[key] ??= [];
            map[key].push(m);
        });
    });

    // Ensure each manga appears in at most one returned group.
    // Iterate over groups and assign mangas to the first group they appear in.
    const usedIds = new Set<string>();
    const result: TMangaDuplicates<TMangaDuplicate> = {};

    // Keep deterministic order by sorting keys
    const keys = Object.keys(map).sort();
    for (let k = 0; k < keys.length; k += 1) {
        const key = keys[k];
        const group = map[key];
        // dedupe mangas inside the group by id while preserving order
        const uniqueById: TMangaDuplicate[] = [];
        const seenInGroup = new Set<string>();
        for (let i = 0; i < group.length; i += 1) {
            const m = group[i];
            const id = String(m.id);
            if (!seenInGroup.has(id)) {
                seenInGroup.add(id);
                uniqueById.push(m);
            }
        }

        // filter out mangas already assigned to previous groups
        const remaining: TMangaDuplicate[] = [];
        for (let i = 0; i < uniqueById.length; i += 1) {
            const m = uniqueById[i];
            const id = String(m.id);
            if (!usedIds.has(id)) {
                remaining.push(m);
            }
        }

        if (remaining.length > 1) {
            const firstNode = remaining[0].trackRecords?.nodes?.[0];
            const trackerId = firstNode?.trackerId ?? 'unknown';
            const remoteId = firstNode?.remoteId ?? '';
            const prettifiedKey = `${trackerId}:${remoteId} (${key})`;
            result[prettifiedKey] = remaining;
            for (let i = 0; i < remaining.length; i += 1) {
                usedIds.add(String(remaining[i].id));
            }
        }
    }

    postMessage(result);
};
