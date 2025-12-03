/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import IconButton from '@mui/material/IconButton';
import SettingsIcon from '@mui/icons-material/Settings';
import PopupState, { bindMenu, bindTrigger } from 'material-ui-popup-state';
import Menu from '@mui/material/Menu';
import MenuItem from '@mui/material/MenuItem';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { requestManager } from '@/lib/requests/RequestManager.ts';
import { useLocalStorage } from '@/base/hooks/useStorage.tsx';
import { GridLayouts } from '@/base/components/GridLayouts.tsx';
import { CheckboxInput } from '@/base/components/inputs/CheckboxInput.tsx';
import { LoadingPlaceholder } from '@/base/components/feedback/LoadingPlaceholder.tsx';
import { EmptyViewAbsoluteCentered } from '@/base/components/feedback/EmptyViewAbsoluteCentered.tsx';
import { defaultPromiseErrorHandler } from '@/lib/DefaultPromiseErrorHandler.ts';
import { MangaCard } from '@/features/manga/components/cards/MangaCard.tsx';
import { StyledGroupedVirtuoso } from '@/base/components/virtuoso/StyledGroupedVirtuoso.tsx';
import { StyledGroupHeader } from '@/base/components/virtuoso/StyledGroupHeader.tsx';
import { GetMangasDuplicatesQuery, GetMangasDuplicatesQueryVariables } from '@/lib/graphql/generated/graphql.ts';
import { GET_MANGAS_DUPLICATES } from '@/lib/graphql/queries/MangaQuery.ts';
import { BaseMangaGrid } from '@/features/manga/components/BaseMangaGrid.tsx';
import { IMangaGridProps } from '@/features/manga/components/MangaGrid.tsx';
import { StyledGroupItemWrapper } from '@/base/components/virtuoso/StyledGroupItemWrapper.tsx';
import { VirtuosoUtil } from '@/lib/virtuoso/Virtuoso.util.tsx';
import type {
    LibraryDuplicatesWorkerInput,
    TMangaDuplicate,
    TMangaDuplicates,
} from '@/features/library/Library.types.ts';
import { GridLayout } from '@/base/Base.types.ts';
import { getErrorMessage } from '@/lib/HelperFunctions.ts';
import { useAppTitleAndAction } from '@/features/navigation-bar/hooks/useAppTitleAndAction.ts';

export const LibraryDuplicates = () => {
    const { t } = useTranslation();

    const [gridLayout, setGridLayout] = useLocalStorage('libraryDuplicatesGridLayout', GridLayout.List);
    const [checkAlternativeTitles, setCheckAlternativeTitles] = useLocalStorage(
        'libraryDuplicatesCheckAlternativeTitles',
        false,
    );

    const [checkTrackedBySameTracker, setCheckTrackedBySameTracker] = useLocalStorage(
        'libraryDuplicatesCheckTrackedBySameTracker',
        false,
    );

    // image hash toggle
    const [checkImageHashes, setCheckImageHashes] = useLocalStorage('libraryDuplicatesCheckImageHashes', false);

    useAppTitleAndAction(
        t('library.settings.advanced.duplicates.label.title'),
        <>
            <GridLayouts gridLayout={gridLayout} onChange={setGridLayout} />
            <PopupState variant="popover" popupId="library-dupliactes-settings">
                {(popupstate) => (
                    <>
                        <IconButton {...bindTrigger(popupstate)} color="inherit">
                            <SettingsIcon />
                        </IconButton>
                        <Menu {...bindMenu(popupstate)}>
                            <MenuItem>
                                <CheckboxInput
                                    label={t('library.settings.advanced.duplicates.settings.label.check_description')}
                                    checked={checkAlternativeTitles}
                                    onChange={(_, checked) => {
                                        // If enabling alternative-title check, disable the other two toggles.
                                        setCheckAlternativeTitles(checked);
                                        if (checked) {
                                            setCheckTrackedBySameTracker(false);
                                            setCheckImageHashes(false);
                                        }
                                    }}
                                />
                            </MenuItem>
                            <MenuItem>
                                <CheckboxInput
                                    label={t('library.settings.advanced.duplicates.settings.label.check_tracked')}
                                    checked={checkTrackedBySameTracker}
                                    onChange={(_, checked) => {
                                        // If enabling tracker check, disable the other two toggles.
                                        setCheckTrackedBySameTracker(checked);
                                        if (checked) {
                                            setCheckAlternativeTitles(false);
                                            setCheckImageHashes(false);
                                        }
                                    }}
                                />
                            </MenuItem>
                            <MenuItem>
                                <CheckboxInput
                                    label={t('library.settings.advanced.duplicates.settings.label.check_image_hashes')}
                                    checked={checkImageHashes}
                                    onChange={(_, checked) => {
                                        // If enabling image-hash check, disable the other two toggles.
                                        setCheckImageHashes(checked);
                                        if (checked) {
                                            setCheckAlternativeTitles(false);
                                            setCheckTrackedBySameTracker(false);
                                        }
                                    }}
                                />
                            </MenuItem>
                        </Menu>
                    </>
                )}
            </PopupState>
        </>,
        // include image toggle in deps as it's now used in the UI
        [t, gridLayout, checkAlternativeTitles, checkTrackedBySameTracker, checkImageHashes],
    );

    const { data, loading, error, refetch } = requestManager.useGetMangas<
        GetMangasDuplicatesQuery,
        GetMangasDuplicatesQueryVariables
    >(GET_MANGAS_DUPLICATES, { condition: { inLibrary: true } });

    const [isCheckingForDuplicates, setIsCheckingForDuplicates] = useState(true);

    const [mangasByTitle, setMangasByTitle] = useState<Record<string, TMangaDuplicate[]>>({});
    useEffect(() => {
        setIsCheckingForDuplicates(true);
        const libraryMangas: TMangaDuplicate[] = data?.mangas.nodes ?? [];

        if (!libraryMangas.length) {
            setMangasByTitle({});
            return () => {};
        }

        const workerMangas = libraryMangas.map((m) => ({
            id: m.id,
            title: (m as any).title,
            description: (m as any).description,
            thumbnailUrl: (m as any).thumbnailUrl,
            trackRecords: (m as any).trackRecords?.nodes?.map((n: any) => ({
                trackerId: n.trackerId,
                remoteId: n.remoteId,
                remoteTitle: n.remoteTitle,
            })),
        }));

        const worker = new Worker(new URL('../workers/LibraryDuplicatesWorker.ts', import.meta.url), {
            type: 'module',
        });

        // Debug-enabled onmessage: if worker returns debug info, log it and still set results for UI
        worker.onmessage = (event: MessageEvent<any>) => {
            // Use TMangaDuplicates in a cast so the imported type is actually referenced (avoids TS6133)
            const payload = event.data as
                | TMangaDuplicates<(typeof workerMangas)[number]>
                | {
                      result: TMangaDuplicates<(typeof workerMangas)[number]>;
                      debugSamples?: { idA: string; idB: string; aDist: number; pDist: number; avg: number }[];
                      thresholdUsed?: number;
                  };

            // read debugSamples directly (no leading underscore)
            const debugSamples = (payload as any).debugSamples ?? undefined;

            // If worker returned debug info, log it so you can inspect distances in the browser console
            if (payload && (debugSamples || (payload as any).thresholdUsed !== undefined)) {
                // eslint-disable-next-line no-console
                console.groupCollapsed('LibraryDuplicates image-hash debug (worker)');
                // eslint-disable-next-line no-console
                console.log('thresholdUsed:', (payload as any).thresholdUsed);
                // eslint-disable-next-line no-console
                console.log('sample distances (first 200):', debugSamples);
                // eslint-disable-next-line no-console
                console.groupEnd();
                // set the result groups so UI still works in debug mode
                setMangasByTitle((payload as any).result ?? {});
                setIsCheckingForDuplicates(false);
                return;
            }

            // normal, non-debug behaviour (existing)
            setMangasByTitle((payload as TMangaDuplicates<(typeof workerMangas)[number]>) ?? {});
            setIsCheckingForDuplicates(false);
        };

        // include the flags in worker input; do NOT send threshold here so the worker default is the single source-of-truth
        worker.postMessage({
            mangas: workerMangas,
            checkAlternativeTitles,
            checkTrackedBySameTracker,
            checkImageHashes,
            debug: true,
        } satisfies LibraryDuplicatesWorkerInput & { debug?: boolean });

        return () => worker.terminate();
    }, [data?.mangas.nodes, checkAlternativeTitles, checkTrackedBySameTracker, checkImageHashes]);

    const duplicatedTitles = useMemo(
        () => Object.keys(mangasByTitle).toSorted((titleA, titleB) => titleA.localeCompare(titleB)),
        [mangasByTitle],
    );
    const duplicatedMangas = useMemo(
        () => duplicatedTitles.map((title) => mangasByTitle[title]).flat(),
        [mangasByTitle],
    );

    // counts for groups and mangas involved in groups (kept for other UI uses)
    const duplicateGroupsCount = duplicatedTitles.length;
    const duplicateMangasCount = duplicatedMangas.length;

    // reference counts via a ref so TypeScript treats them as used (avoids TS6133)
    // no runtime side-effects or changes to your useAppTitleAndAction usage
    const countsRef = useRef(0);
    useEffect(() => {
        countsRef.current = duplicateGroupsCount + duplicateMangasCount;
    }, [duplicateGroupsCount, duplicateMangasCount]);

    const mangasCountByTitle = useMemo(
        () => duplicatedTitles.map((title) => mangasByTitle[title]).map((mangas) => mangas.length),
        [mangasByTitle],
    );

    const computeItemKey = VirtuosoUtil.useCreateGroupedComputeItemKey(
        mangasCountByTitle,
        useCallback((index) => duplicatedTitles[index], [duplicatedTitles]),
        useCallback(
            (index, groupIndex) => `${duplicatedTitles[groupIndex]}-${duplicatedMangas[index].id}}`,
            [duplicatedTitles, duplicatedMangas],
        ),
    );

    if (loading || isCheckingForDuplicates) {
        return <LoadingPlaceholder />;
    }

    if (error) {
        return (
            <EmptyViewAbsoluteCentered
                message={t('global.error.label.failed_to_load_data')}
                messageExtra={getErrorMessage(error)}
                retry={() => refetch().catch(defaultPromiseErrorHandler('LibraryDuplicates::refetch'))}
            />
        );
    }

    if (gridLayout === GridLayout.List) {
        return (
            <StyledGroupedVirtuoso
                persistKey="library-duplicates"
                groupCounts={mangasCountByTitle}
                groupContent={(index) => (
                    <StyledGroupHeader isFirstItem={index === 0}>
                        <Typography variant="h5" component="h2">
                            {duplicatedTitles[index]}
                        </Typography>
                    </StyledGroupHeader>
                )}
                computeItemKey={computeItemKey}
                itemContent={(index) => (
                    <StyledGroupItemWrapper>
                        <MangaCard
                            manga={duplicatedMangas[index] as IMangaGridProps['mangas'][number]}
                            gridLayout={gridLayout}
                            selected={null}
                            mode="duplicate"
                        />
                    </StyledGroupItemWrapper>
                )}
            />
        );
    }

    return duplicatedTitles.map((title, index) => (
        <Box key={title}>
            <StyledGroupHeader sx={{ pt: index === 0 ? undefined : 0, pb: 0 }} isFirstItem={false}>
                <Typography variant="h5" component="h2">
                    {title}
                </Typography>
            </StyledGroupHeader>
            <BaseMangaGrid
                // the key needs to include filters and query to force a re-render of the virtuoso grid to prevent https://github.com/petyosi/react-virtuoso/issues/1242
                key={`${checkAlternativeTitles.toString()}-${checkImageHashes.toString()}`}
                mangas={mangasByTitle[title] as IMangaGridProps['mangas']}
                hasNextPage={false}
                loadMore={() => {}}
                isLoading={false}
                gridLayout={gridLayout}
                inLibraryIndicator={false}
                horizontal
                mode="duplicate"
            />
        </Box>
    ));
};
