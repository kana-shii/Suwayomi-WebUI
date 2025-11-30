/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import { MangaStatus, MangaType, TrackerType } from '@/lib/graphql/generated/graphql.ts';
import { GridLayout } from '@/base/Base.types.ts';

export type MetadataLibrarySettings = {
    showAddToLibraryCategorySelectDialog: boolean;
    ignoreFilters: boolean;
    removeMangaFromCategories: boolean;
    showTabSize: boolean;
    showContinueReadingButton: boolean;
    showDownloadBadge: boolean;
    showUnreadBadge: boolean;
    gridLayout: GridLayout;
};
export type LibrarySortMode =
    | 'unreadChapters'
    | 'totalChapters'
    | 'alphabetically'
    | 'dateAdded'
    | 'lastRead'
    | 'latestFetchedChapter'
    | 'latestUploadedChapter';

export interface LibraryOptions {
    // sort options
    sortBy: NullAndUndefined<LibrarySortMode>;
    sortDesc: NullAndUndefined<boolean>;

    // filter options
    hasDownloadedChapters: NullAndUndefined<boolean>;
    hasBookmarkedChapters: NullAndUndefined<boolean>;
    hasUnreadChapters: NullAndUndefined<boolean>;
    hasReadChapters: NullAndUndefined<boolean>;
    hasDuplicateChapters: NullAndUndefined<boolean>;
    hasTrackerBinding: Record<TrackerType['id'], NullAndUndefined<boolean>>;
    hasStatus: Record<MangaStatus, NullAndUndefined<boolean>>;
}

// Minimal track-record node shape used by the duplicates workers / UI.
// Avoids referencing generated GraphQL node keys directly (which may differ between schemas)
// so we can include optional fields like remoteTitle without TypeScript Pick mismatches.
export type TTrackRecordNodeMin = {
    id?: string | number | null;
    trackerId?: string | number | null;
    remoteId?: string | null;
    // remoteTitle may not be present in generated types for all schemas/graphQL setups;
    // keep it optional and typed as string|null.
    remoteTitle?: string | null;
};

// Include trackRecords and thumbnailUrl for hashing and worker UI
export type TMangaDuplicate = Pick<MangaType, 'id' | 'title' | 'description' | 'thumbnailUrl'> & {
    trackRecords?: { nodes: TTrackRecordNodeMin[] } | null;
};

export type TMangaDuplicates<Manga> = Record<string, Manga[]>;

export type TMangaDuplicateResult<Manga> = { byTitle: Manga[]; byAlternativeTitle: Manga[] };

export type LibraryDuplicatesWorkerInput<Manga extends TMangaDuplicate = TMangaDuplicate> = {
    mangas: Manga[];
    checkAlternativeTitles: boolean;
    checkTrackedBySameTracker?: boolean;
    checkImageHashes?: boolean;
};

export type LibraryDuplicatesDescriptionWorkerInput<Manga extends TMangaDuplicate = TMangaDuplicate> = {
    mangasToCheck: Manga[];
    mangas: Manga[];
};

export type LibraryOptionsContextType = {
    options: LibraryOptions;
    setOptions: React.Dispatch<React.SetStateAction<LibraryOptions>>;
};
