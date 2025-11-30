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
    for (const key of keys) {
        const group = map[key];
        // dedupe mangas inside the group by id while preserving order
        const uniqueById: TMangaDuplicate[] = [];
        const seenInGroup = new Set<string>();
        for (let i = 0; i < group.length; i++) {
            const m = group[i];
            const id = String(m.id);
            if (!seenInGroup.has(id)) {
                seenInGroup.add(id);
                uniqueById.push(m);
            }
        }

        // filter out mangas already assigned to previous groups
        const remaining = uniqueById.filter((m) => !usedIds.has(String(m.id)));

        if (remaining.length > 1) {
            // Prefer the remoteTitle for the tracker entry name, fall back to remoteId/trackerId if absent
            const firstNode = remaining[0].trackRecords?.nodes?.[0];
            const entryName =
                firstNode?.remoteTitle ??
                firstNode?.remoteId ??
                `${firstNode?.trackerId ?? 'unknown'}:${firstNode?.remoteId ?? ''}`;
            const prettifiedKey = `${entryName} (${key})`;
            result[prettifiedKey] = remaining;
            remaining.forEach((m) => usedIds.add(String(m.id)));
        }
    }

    postMessage(result);
};
