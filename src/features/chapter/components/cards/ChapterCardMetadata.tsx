/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import Box from '@mui/material/Box';
import type { ComponentProps, ReactNode } from 'react';
import Stack from '@mui/material/Stack';
import { TypographyMaxLines } from '@/base/components/texts/TypographyMaxLines.tsx';
import { CustomTooltip } from '@/base/components/CustomTooltip.tsx';
import { useTheme } from '@mui/material/styles';

export const ChapterCardMetadata = ({
    title,
    secondaryText,
    ternaryText,
    infoIcons,
    showUnreadDot,
    disableTooltips,
    slotProps,
}: {
    title: string;
    secondaryText?: string | null;
    ternaryText?: string | null;
    infoIcons?: ReactNode;
    showUnreadDot?: boolean;
    disableTooltips?: boolean;
    slotProps?: {
        title?: ComponentProps<typeof TypographyMaxLines>;
        secondaryText?: ComponentProps<typeof TypographyMaxLines>;
        ternaryText?: ComponentProps<typeof TypographyMaxLines>;
    };
}) => {
    const theme = useTheme();
    const unreadDotSize = `calc(${theme.typography.caption.fontSize} * 0.6)`;

    return (
        <Box
            sx={{
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'center',
                flexGrow: 1,
                flexShrink: 1,
                wordBreak: 'break-word',
            }}
        >
            <Stack
                sx={{
                    flexDirection: 'row',
                    gap: 0.5,
                    alignItems: 'center',
                }}
            >
                {infoIcons}
                {disableTooltips ? (
                    <TypographyMaxLines variant="h6" component="h3" {...slotProps?.title}>
                        {title}
                    </TypographyMaxLines>
                ) : (
                    <CustomTooltip title={title}>
                        <TypographyMaxLines variant="h6" component="h3" {...slotProps?.title}>
                            {title}
                        </TypographyMaxLines>
                    </CustomTooltip>
                )}
            </Stack>

            {secondaryText && (
                <Stack
                    sx={{
                        flexDirection: 'row',
                        alignItems: 'center',
                        gap: 0.75,
                        minWidth: 0,
                    }}
                >
                    {showUnreadDot ? (
                        <Box
                            sx={{
                                width: unreadDotSize,
                                height: unreadDotSize,
                                borderRadius: '50%',
                                backgroundColor: 'primary.main',
                                flex: '0 0 auto',
                            }}
                        />
                    ) : null}

                    {disableTooltips ? (
                        <TypographyMaxLines
                            variant="caption"
                            display="block"
                            lines={1}
                            {...slotProps?.secondaryText}
                            sx={{ maxWidth: 'fit-content', ...slotProps?.secondaryText?.sx }}
                        >
                            {secondaryText}
                        </TypographyMaxLines>
                    ) : (
                        <CustomTooltip title={secondaryText}>
                            <TypographyMaxLines
                                variant="caption"
                                display="block"
                                lines={1}
                                {...slotProps?.secondaryText}
                                sx={{ maxWidth: 'fit-content', ...slotProps?.secondaryText?.sx }}
                            >
                                {secondaryText}
                            </TypographyMaxLines>
                        </CustomTooltip>
                    )}
                </Stack>
            )}

            {ternaryText &&
                (disableTooltips ? (
                    <TypographyMaxLines
                        variant="caption"
                        display="block"
                        lines={1}
                        {...slotProps?.ternaryText}
                        sx={{ maxWidth: 'fit-content', ...slotProps?.ternaryText?.sx }}
                    >
                        {ternaryText}
                    </TypographyMaxLines>
                ) : (
                    <CustomTooltip title={ternaryText}>
                        <TypographyMaxLines
                            variant="caption"
                            display="block"
                            lines={1}
                            {...slotProps?.ternaryText}
                            sx={{ maxWidth: 'fit-content', ...slotProps?.ternaryText?.sx }}
                        >
                            {ternaryText}
                        </TypographyMaxLines>
                    </CustomTooltip>
                ))}
        </Box>
    );
};
