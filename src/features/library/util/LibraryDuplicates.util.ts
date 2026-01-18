/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import { MangaType } from '@/lib/graphql/generated/graphql.ts';
import { enhancedCleanup } from '@/base/utils/Strings.ts';
import { TMangaDuplicate, TMangaDuplicateResult, TMangaDuplicates } from '@/features/library/Library.types.ts';

export const findDuplicatesByTitle = <Manga extends Pick<MangaType, 'title'>>(
    libraryMangas: Manga[],
): TMangaDuplicates<Manga> => {
    const titleToMangas = Object.groupBy(libraryMangas, ({ title }) => enhancedCleanup(title));

    return Object.fromEntries(
        Object.entries(titleToMangas)
            .filter((titleToMangaMap): titleToMangaMap is [string, Manga[]] => (titleToMangaMap[1]?.length ?? 0) > 1)
            .map(([, mangas]) => [mangas[0].title, mangas]),
    );
};

const findDuplicatesByTitleAndAlternativeTitlesSingleManga = <Manga extends TMangaDuplicate>(
    manga: Manga,
    mangas: Manga[],
): TMangaDuplicateResult<Manga> => {
    const titleToCheck = enhancedCleanup(manga.title);

    const result: ReturnType<typeof findDuplicatesByTitleAndAlternativeTitlesSingleManga<Manga>> = {
        byTitle: [manga],
        byAlternativeTitle: [manga],
    };

    mangas.forEach((libraryManga) => {
        const isDifferentManga = manga.id !== libraryManga.id;
        if (!isDifferentManga) {
            return;
        }

        const doesTitleMatch = enhancedCleanup(libraryManga.title) === titleToCheck;
        const doesAlternativeTitleMatch = enhancedCleanup(libraryManga?.description ?? '').includes(titleToCheck);

        const isDuplicate = doesTitleMatch || doesAlternativeTitleMatch;
        if (!isDuplicate) {
            return;
        }

        if (doesTitleMatch) {
            result.byTitle.push(libraryManga);
        }

        if (doesAlternativeTitleMatch) {
            result.byAlternativeTitle.push(libraryManga);
        }
    });

    return result;
};

export const findDuplicatesByTitleAndAlternativeTitles = <Manga extends TMangaDuplicate>(
    mangasToCheck: Manga[],
    mangas: Manga[] = mangasToCheck,
): TMangaDuplicates<Manga> => {
    const titleToMangas: TMangaDuplicates<Manga> = {};
    const titleToAlternativeTitleMatches: TMangaDuplicates<Manga> = {};

    mangasToCheck.forEach((mangaToCheck) => {
        const titleToCheck = enhancedCleanup(mangaToCheck.title);

        titleToMangas[titleToCheck] ??= [];
        titleToAlternativeTitleMatches[titleToCheck] ??= [];

        const { byTitle, byAlternativeTitle } = findDuplicatesByTitleAndAlternativeTitlesSingleManga(
            mangaToCheck,
            mangas,
        );

        titleToMangas[titleToCheck].push(...byTitle);
        titleToAlternativeTitleMatches[titleToCheck].push(...byAlternativeTitle);
    });

    const titleToDuplicatesEntries = Object.entries(titleToMangas)
        .map(([title, titleMatches]) => {
            const uniqueTitleMatches = new Set(titleMatches);
            const uniqueAlternativeTitleMatches = new Set(titleToAlternativeTitleMatches[title] ?? []);

            const originalTitle = [...uniqueTitleMatches][0].title;

            const combinedDuplicates = [...uniqueTitleMatches, ...uniqueAlternativeTitleMatches];
            const duplicates = [...new Set([...combinedDuplicates])];

            const noDuplicatesFound = duplicates.length === 1;
            if (noDuplicatesFound) {
                return null;
            }

            return [originalTitle, duplicates];
        })
        .filter((entry) => !!entry);

    return Object.fromEntries(titleToDuplicatesEntries);
};

/* ---------------------------------------
 * 1 = Suwayomi method (title + alternative titles)
 * 2 = Exact title match
 * 3 = Fuzzy title (word-boundary, case-insensitive)
 * 4 = Title substring (case-insensitive)
 * -------------------------------------- */
export type DuplicateMatchMode = 1 | 2 | 3 | 4;
export const DUPLICATE_MATCH_MODE: DuplicateMatchMode = 1;

/* -------- Exact title match -------- */
export const findDuplicatesExactTitle = <Manga extends Pick<MangaType, 'title'>>(
    libraryMangas: Manga[],
): TMangaDuplicates<Manga> => {
    const titleToMangas = Object.groupBy(libraryMangas, ({ title }) => title);

    return Object.fromEntries(
        Object.entries(titleToMangas)
            .filter((titleToMangaMap): titleToMangaMap is [string, Manga[]] => (titleToMangaMap[1]?.length ?? 0) > 1)
            .map(([, mangas]) => [mangas[0].title, mangas]),
    );
};

/* -------- Fuzzy title (word-boundary) -------- */
const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

export const findDuplicatesFuzzyTitle = <Manga extends TMangaDuplicate>(
    mangasToCheck: Manga[],
    libraryMangas: Manga[] = mangasToCheck,
): TMangaDuplicates<Manga> => {
    const titleToDuplicates: TMangaDuplicates<Manga> = {};

    mangasToCheck.forEach((mangaToCheck) => {
        const rawTitle = mangaToCheck.title;
        if (!rawTitle) {
            return;
        }

        const escaped = escapeRegExp(rawTitle);
        const regex = new RegExp(`(^|\\b)${escaped}(\\b|$)`, 'i');

        const matches: Manga[] = [];
        libraryMangas.forEach((candidate) => {
            const isDifferent = candidate.id !== (mangaToCheck as TMangaDuplicate).id;
            if (!isDifferent) {
                return;
            }

            if (regex.test(candidate.title)) {
                matches.push(candidate);
            }
        });

        const unique = [...new Set([mangaToCheck, ...matches])];
        if (unique.length > 1) {
            titleToDuplicates[rawTitle] = unique;
        }
    });

    return titleToDuplicates;
};

/* -------- Title substring (case-insensitive) -------- */
export const findDuplicatesTitleSubstring = <Manga extends TMangaDuplicate>(
    mangasToCheck: Manga[],
    libraryMangas: Manga[] = mangasToCheck,
): TMangaDuplicates<Manga> => {
    const titleToDuplicates: TMangaDuplicates<Manga> = {};

    mangasToCheck.forEach((mangaToCheck) => {
        const rawTitle = mangaToCheck.title;
        if (!rawTitle) {
            return;
        }

        const needle = rawTitle.toLowerCase();

        const matches: Manga[] = [];
        libraryMangas.forEach((candidate) => {
            const isDifferent = candidate.id !== (mangaToCheck as TMangaDuplicate).id;
            if (!isDifferent) {
                return;
            }

            if (candidate.title.toLowerCase().includes(needle)) {
                matches.push(candidate);
            }
        });

        const unique = [...new Set([mangaToCheck, ...matches])];
        if (unique.length > 1) {
            titleToDuplicates[rawTitle] = unique;
        }
    });

    return titleToDuplicates;
};

/* -------- Entry point that switches by const (via strategy map to avoid literal comparisons) -------- */
type StrategyFn = <Manga extends TMangaDuplicate>(
    mangasToCheck: Manga[],
    libraryMangas?: Manga[],
) => TMangaDuplicates<Manga>;

const STRATEGIES: Record<DuplicateMatchMode, StrategyFn> = {
    1: <Manga extends TMangaDuplicate>(mangasToCheck: Manga[], libraryMangas: Manga[] = mangasToCheck) =>
        findDuplicatesByTitleAndAlternativeTitles(mangasToCheck, libraryMangas),
    2: <Manga extends TMangaDuplicate>(mangasToCheck: Manga[]) => findDuplicatesExactTitle(mangasToCheck),
    3: <Manga extends TMangaDuplicate>(mangasToCheck: Manga[], libraryMangas: Manga[] = mangasToCheck) =>
        findDuplicatesFuzzyTitle(mangasToCheck, libraryMangas),
    4: <Manga extends TMangaDuplicate>(mangasToCheck: Manga[], libraryMangas: Manga[] = mangasToCheck) =>
        findDuplicatesTitleSubstring(mangasToCheck, libraryMangas),
};

export const findDuplicateMangas = <Manga extends TMangaDuplicate>(
    mangasToCheck: Manga[],
    libraryMangas: Manga[] = mangasToCheck,
): TMangaDuplicates<Manga> => {
    const strategy = STRATEGIES[DUPLICATE_MATCH_MODE] as StrategyFn;
    return strategy(mangasToCheck, libraryMangas) as TMangaDuplicates<Manga>;
};
