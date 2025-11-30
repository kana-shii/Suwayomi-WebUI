/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import IconButton from '@mui/material/IconButton';
import { useTranslation } from 'react-i18next';
import { memo } from 'react';
import VisibilityIcon from '@mui/icons-material/Visibility';
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff';
import { CustomTooltip } from '@/base/components/CustomTooltip.tsx';
import { Chapters } from '@/features/chapter/services/Chapters.ts';
import { ChapterAction, TChapterReader } from '@/features/chapter/Chapter.types.ts';
import { CHAPTER_ACTION_TO_TRANSLATION } from '@/features/chapter/Chapter.constants.ts';

const BaseReaderFillermarkButton = ({ id, isFillermarked }: Pick<TChapterReader, 'id' | 'isFillermarked'>) => {
    const { t } = useTranslation();

    const fillermarkAction: Extract<ChapterAction, 'unfillermark' | 'fillermark'> = isFillermarked
        ? 'unfillermark'
        : 'fillermark';

    return (
        <CustomTooltip title={t(CHAPTER_ACTION_TO_TRANSLATION[fillermarkAction].action.single)}>
            <IconButton onClick={() => Chapters.performAction(fillermarkAction, [id], {})} color="inherit">
                {isFillermarked ? <VisibilityIcon /> : <VisibilityOffIcon />}
            </IconButton>
        </CustomTooltip>
    );
};

export const ReaderFillermarkButton = memo(BaseReaderFillermarkButton);
