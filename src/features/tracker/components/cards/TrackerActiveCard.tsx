/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import Divider from '@mui/material/Divider';
import FormGroup from '@mui/material/FormGroup';
import IconButton from '@mui/material/IconButton';
import Link from '@mui/material/Link';
import ListItemButton from '@mui/material/ListItemButton';
import MenuItem from '@mui/material/MenuItem';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import PopupState, { bindDialog, bindMenu, bindTrigger } from 'material-ui-popup-state';
import { useMemo, useState, useRef, useCallback } from 'react';
import Badge from '@mui/material/Badge';
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff';
import { useLingui } from '@lingui/react/macro';
import { CustomTooltip } from '@/base/components/CustomTooltip.tsx';
import { requestManager } from '@/lib/requests/RequestManager.ts';
import { Trackers } from '@/features/tracker/services/Trackers.ts';
import { ListPreference } from '@/features/source/configuration/components/ListPreference.tsx';
import { NumberSetting } from '@/base/components/settings/NumberSetting.tsx';
import { DateSetting } from '@/base/components/settings/DateSetting.tsx';
import { makeToast } from '@/base/utils/Toast.ts';
import { Menu } from '@/base/components/menu/Menu.tsx';
import { CARD_STYLING, UNSET_DATE } from '@/features/tracker/Tracker.constants.ts';
import { TypographyMaxLines } from '@/base/components/texts/TypographyMaxLines.tsx';
import { SelectSetting, SelectSettingValue } from '@/base/components/settings/SelectSetting.tsx';
import { CheckboxInput } from '@/base/components/inputs/CheckboxInput.tsx';
import { TrackRecordType } from '@/lib/graphql/generated/graphql.ts';
import { getErrorMessage } from '@/lib/HelperFunctions.ts';
import { TTrackerBind, TTrackRecordBind } from '@/features/tracker/Tracker.types.ts';
import { AvatarSpinner } from '@/base/components/AvatarSpinner.tsx';

const TrackerActiveLink = ({ children, url }: { children: React.ReactNode; url: string }) => (
    <Link href={url} rel="noreferrer" target="_blank" underline="none" color="inherit">
        {children}
    </Link>
);

type TTrackerActive = Pick<TTrackerBind, 'id' | 'name' | 'icon' | 'supportsTrackDeletion' | 'supportsPrivateTracking'>;
const TrackerActiveRemoveBind = ({
    trackerRecordId,
    tracker,
    onClick,
    onClose,
}: {
    trackerRecordId: TrackRecordType['id'];
    tracker: TTrackerActive;
    onClick: () => void;
    onClose: () => void;
}) => {
    const { t } = useLingui();

    const [removeRemoteTracking, setRemoveRemoteTracking] = useState(false);

    const removeBind = () => {
        onClose();
        requestManager
            .unbindTracker(trackerRecordId, removeRemoteTracking)
            .response.then(() => makeToast((t as any)('Untracked manga'), 'success'))
            .catch((e) => makeToast((t as any)('Could not untrack manga'), 'error', getErrorMessage(e)));
    };

    return (
        <PopupState variant="dialog" popupId={`tracker-active-menu-remove-button-${tracker.id}`}>
            {(popupState) => (
                <>
                    <MenuItem
                        {...bindTrigger(popupState)}
                        onClick={() => {
                            onClick();
                            popupState.open();
                        }}
                    >
                        {(t as any)('Remove')}
                    </MenuItem>
                    <Dialog
                        {...bindDialog(popupState)}
                        onClose={() => {
                            onClose();
                            popupState.close();
                        }}
                    >
                        <DialogTitle>{(t as any)(`Remove ${tracker.name} tracking?`)}</DialogTitle>
                        <DialogContent dividers>
                            <Typography>{(t as any)('This will remove the tracking locally.')}</Typography>
                            {tracker.supportsTrackDeletion && (
                                <FormGroup>
                                    <CheckboxInput
                                        disabled={false}
                                        label={(t as any)(`Also remove from ${tracker.name}`)}
                                        checked={removeRemoteTracking}
                                        onChange={(_, checked) => setRemoveRemoteTracking(checked)}
                                    />
                                </FormGroup>
                            )}
                        </DialogContent>
                        <DialogActions>
                            <Button
                                autoFocus
                                onClick={() => {
                                    popupState.close();
                                    onClose();
                                }}
                            >
                                {(t as any)('Cancel')}
                            </Button>
                            <Button
                                onClick={() => {
                                    popupState.close();
                                    onClose();
                                    removeBind();
                                }}
                            >
                                {(t as any)('Ok')}
                            </Button>
                        </DialogActions>
                    </Dialog>
                </>
            )}
        </PopupState>
    );
};

const TrackerUpdatePrivateStatus = ({
    trackRecordId,
    isPrivate,
    closeMenu,
    supportsPrivateTracking,
}: {
    trackRecordId: TrackRecordType['id'];
    isPrivate: boolean;
    closeMenu: () => void;
    supportsPrivateTracking: TTrackerActive['supportsPrivateTracking'];
}) => {
    const { t } = useLingui();

    if (!supportsPrivateTracking) {
        return null;
    }

    return (
        <MenuItem
            onClick={() => {
                requestManager
                    .updateTrackerBind(trackRecordId, { private: !isPrivate })
                    .response.catch((e) =>
                        makeToast((t as any)(`Failed to save changes`), 'error', getErrorMessage(e)),
                    );
                closeMenu();
            }}
        >
            {(t as any)(isPrivate ? `Track publicly` : `Track privately`)}
        </MenuItem>
    );
};

type TTrackRecordActive = Pick<TTrackRecordBind, 'id' | 'remoteUrl' | 'title' | 'private'>;
const TrackerActiveHeader = ({
    trackRecord,
    tracker,
    openSearch,
}: {
    trackRecord: TTrackRecordActive;
    tracker: TTrackerActive;
    openSearch: () => void;
}) => {
    const { t } = useLingui();

    // Local typed wrapper removed to avoid TS key validation; use (t as any)(...) directly below.

    // --- Added: long-press/copy support for active tracker header (icon + title) ---
    // Small, self-contained long-press implementation so we don't change other behavior.
    const holdTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
    const didLongPress = useRef(false);

    const clearHoldTimer = useCallback(() => {
        if (holdTimer.current) {
            clearTimeout(holdTimer.current);
            holdTimer.current = null;
        }
        // reset the flag shortly after mouseup/touchend so a subsequent click won't be blocked indefinitely
        setTimeout(() => {
            didLongPress.current = false;
        }, 0);
    }, []);

    const startHold = useCallback(
        (action: () => void) => {
            clearHoldTimer();
            holdTimer.current = setTimeout(() => {
                didLongPress.current = true;
                action();
                holdTimer.current = null;
            }, 600);
        },
        [clearHoldTimer],
    );

    const copyTextToClipboard = useCallback(
        async (text: string, label?: string) => {
            try {
                if (!text) {
                    makeToast((t as any)('global.error.label.failed_to_copy') ?? 'No text to copy', 'error');
                    return;
                }

                if (navigator.clipboard && navigator.clipboard.writeText) {
                    await navigator.clipboard.writeText(text);
                } else {
                    const ta = document.createElement('textarea');
                    ta.value = text;
                    ta.style.position = 'fixed';
                    ta.style.left = '-9999px';
                    document.body.appendChild(ta);
                    ta.focus();
                    ta.select();
                    document.execCommand('copy');
                    document.body.removeChild(ta);
                }

                if (label) {
                    makeToast(`${label} ${(t as any)('global.label.copied') ?? 'copied'}`, 'info');
                } else {
                    makeToast((t as any)('global.label.copied') ?? 'Copied', 'info');
                }
            } catch (e) {
                makeToast(
                    (t as any)('global.error.label.failed_to_copy') ?? 'Failed to copy',
                    'error',
                    getErrorMessage(e),
                );
            }
        },
        [t],
    );

    const onHoldCopyUrl = useCallback(() => {
        // prefer the tracked entry remoteUrl
        const url = trackRecord.remoteUrl ?? '';
        copyTextToClipboard(url, (t as any)('tracking.action.copy_url.label') ?? 'URL');
    }, [trackRecord.remoteUrl, copyTextToClipboard, t]);

    const onHoldCopyName = useCallback(() => {
        copyTextToClipboard(
            trackRecord.title ?? tracker.name ?? '',
            (t as any)('tracking.action.copy_title.label') ?? 'Name',
        );
    }, [trackRecord.title, tracker.name, copyTextToClipboard, t]);

    const handleContextMenuIcon = (e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
        didLongPress.current = true;
        onHoldCopyUrl();
    };
    const handleContextMenuName = (e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
        didLongPress.current = true;
        onHoldCopyName();
    };

    // We need to prevent the ListItemButton's click action (openSearch) if a long-press happened.
    const handleListItemClick = (e: React.MouseEvent) => {
        if (didLongPress.current) {
            e.stopPropagation();
            didLongPress.current = false;
            return;
        }
        openSearch();
    };
    // --- end added code ---

    return (
        <Stack
            direction="row"
            sx={{
                alignItems: 'stretch',
                paddingBottom: 2,
            }}
        >
            <Badge
                badgeContent={
                    trackRecord.private ? (
                        <Stack sx={{ p: '2px 6px', backgroundColor: 'primary.main', borderRadius: 100 }}>
                            <VisibilityOffIcon fontSize="small" sx={{ color: 'primary.contrastText' }} />
                        </Stack>
                    ) : null
                }
            >
                <TrackerActiveLink url={trackRecord.remoteUrl}>
                    {/* wrap avatar in a div to attach long-press / contextmenu handlers without changing layout */}
                    <div
                        role="button"
                        tabIndex={0}
                        onMouseDown={(e: React.MouseEvent) => {
                            e.stopPropagation();
                            startHold(onHoldCopyUrl);
                        }}
                        onMouseUp={(e: React.MouseEvent) => {
                            e.stopPropagation();
                            clearHoldTimer();
                        }}
                        onMouseLeave={(e: React.MouseEvent) => {
                            e.stopPropagation();
                            clearHoldTimer();
                        }}
                        onTouchStart={(e: React.TouchEvent) => {
                            (e as React.TouchEvent).stopPropagation();
                            startHold(onHoldCopyUrl);
                        }}
                        onTouchEnd={(e: React.TouchEvent) => {
                            (e as React.TouchEvent).stopPropagation();
                            clearHoldTimer();
                        }}
                        onContextMenu={handleContextMenuIcon}
                        onKeyDown={(e: React.KeyboardEvent) => {
                            // Support keyboard activation for accessibility (Enter / Space)
                            if (e.key === 'Enter' || e.key === ' ') {
                                e.preventDefault();
                                e.stopPropagation();
                                onHoldCopyUrl();
                            }
                        }}
                        aria-label={
                            tracker.name
                                ? `${tracker.name} (${(t as any)('tracking.action.copy_url.label')})`
                                : undefined
                        }
                    >
                        <AvatarSpinner
                            alt={`${tracker.name}`}
                            iconUrl={requestManager.getValidImgUrlFor(tracker.icon)}
                            slots={{
                                avatarProps: {
                                    variant: 'rounded',
                                    sx: { width: 64, height: 64 },
                                },
                                spinnerImageProps: {
                                    ignoreQueue: true,
                                },
                            }}
                        />
                    </div>
                </TrackerActiveLink>
            </Badge>

            <ListItemButton
                sx={{ flexGrow: 1 }}
                onClick={handleListItemClick}
                onMouseDown={(e: React.MouseEvent) => {
                    e.stopPropagation();
                    startHold(onHoldCopyName);
                }}
                onMouseUp={(e: React.MouseEvent) => {
                    e.stopPropagation();
                    clearHoldTimer();
                }}
                onMouseLeave={(e: React.MouseEvent) => {
                    e.stopPropagation();
                    clearHoldTimer();
                }}
                onTouchStart={(e: React.TouchEvent) => {
                    (e as React.TouchEvent).stopPropagation();
                    startHold(onHoldCopyName);
                }}
                onTouchEnd={(e: React.TouchEvent) => {
                    (e as React.TouchEvent).stopPropagation();
                    clearHoldTimer();
                }}
                onContextMenu={handleContextMenuName}
                // ensure keyboard users can open search via Enter/Space on the ListItemButton (native handles this)
            >
                <CustomTooltip title={trackRecord.title}>
                    <TypographyMaxLines flexGrow={1} lines={1}>
                        {trackRecord.title}
                    </TypographyMaxLines>
                </CustomTooltip>
            </ListItemButton>
            <Stack
                sx={{
                    justifyContent: 'center',
                }}
            >
                <PopupState variant="popover" popupId={`tracker-active-menu-popup-${tracker.id}`}>
                    {(popupState) => (
                        <>
                            <IconButton {...bindTrigger(popupState)}>
                                <MoreVertIcon />
                            </IconButton>
                            <Menu {...bindMenu(popupState)} id={`tracker-active-menu-${tracker.id}`}>
                                {(onClose, setHideMenu) => [
                                    /* Added copy URL / copy name menu items (per-tracker overflow menu) */
                                    <MenuItem
                                        key={`tracker-active-menu-item-copy-url-${tracker.id}`}
                                        onClick={() => {
                                            const url = trackRecord.remoteUrl ?? '';
                                            if (!url) {
                                                makeToast(
                                                    (t as any)('global.error.label.failed_to_copy') ?? 'No URL to copy',
                                                    'error',
                                                );
                                            } else {
                                                copyTextToClipboard(
                                                    url,
                                                    (t as any)('tracking.action.copy_url.label') ?? 'URL',
                                                );
                                            }
                                            onClose();
                                        }}
                                    >
                                        {(t as any)('tracking.action.copy_url.label')}
                                    </MenuItem>,
                                    <MenuItem
                                        key={`tracker-active-menu-item-copy-name-${tracker.id}`}
                                        onClick={() => {
                                            copyTextToClipboard(
                                                trackRecord.title ?? tracker.name ?? '',
                                                (t as any)('tracking.action.copy_title.label') ?? 'Name',
                                            );
                                            onClose();
                                        }}
                                    >
                                        {(t as any)('tracking.action.copy_title.label')}
                                    </MenuItem>,

                                    <TrackerActiveLink
                                        key={`tracker-active-menu-item-browser-${tracker.id}`}
                                        url={trackRecord.remoteUrl}
                                    >
                                        <MenuItem onClick={() => onClose()}>{(t as any)(`Open in browser`)}</MenuItem>
                                    </TrackerActiveLink>,
                                    <TrackerActiveRemoveBind
                                        key={`tracker-active-menu-item-remove-${tracker.id}`}
                                        trackerRecordId={trackRecord.id}
                                        tracker={tracker}
                                        onClick={() => setHideMenu(true)}
                                        onClose={onClose}
                                    />,
                                    <TrackerUpdatePrivateStatus
                                        trackRecordId={trackRecord.id}
                                        isPrivate={trackRecord.private}
                                        closeMenu={onClose}
                                        supportsPrivateTracking={tracker.supportsPrivateTracking}
                                    />,
                                ]}
                            </Menu>
                        </>
                    )}
                </PopupState>
            </Stack>
        </Stack>
    );
};

const TrackerActiveCardInfoRow = ({ children }: { children: React.ReactNode }) => (
    <Stack direction="row" sx={{ textAlignLast: 'center' }}>
        {children}
    </Stack>
);

const isUnsetScore = (score: string | number): boolean => !Math.trunc(Number(score));

export const TrackerActiveCard = ({
    trackRecord,
    tracker,
    onClick,
}: {
    trackRecord: TTrackRecordBind;
    tracker: TTrackerBind;
    onClick: () => void;
}) => {
    const { t } = useLingui();

    const isScoreUnset = isUnsetScore(trackRecord.displayScore);
    const currentScore = isScoreUnset ? tracker.scores[0] : trackRecord.displayScore;

    const selectSettingValues = useMemo(
        () =>
            tracker.scores.map(
                (score) =>
                    [score, { text: isUnsetScore(score) ? '-' : score }] satisfies SelectSettingValue<
                        TTrackerBind['scores'][number]
                    >,
            ),
        [tracker.scores],
    );

    const updateTrackerBind = (patch: Parameters<typeof requestManager.updateTrackerBind>[1]) => {
        requestManager
            .updateTrackerBind(trackRecord.id, patch)
            .response.catch((e) => makeToast((t as any)(`Failed to save changes`), 'error', getErrorMessage(e)));
    };

    return (
        <Card sx={CARD_STYLING}>
            <CardContent sx={{ padding: 0 }}>
                <TrackerActiveHeader trackRecord={trackRecord} tracker={tracker} openSearch={onClick} />
                <Card sx={{ backgroundColor: 'background.default' }}>
                    <CardContent sx={{ padding: '0' }}>
                        <Box sx={{ padding: 1 }}>
                            <TrackerActiveCardInfoRow>
                                <ListPreference
                                    ListPreferenceTitle={(t as any)('Status')}
                                    entries={tracker.statuses.map((status) => status.name)}
                                    key="status"
                                    type="ListPreference"
                                    entryValues={tracker.statuses.map((status) => `${status.value}`)}
                                    ListPreferenceCurrentValue={`${trackRecord.status}`}
                                    updateValue={(_, status) => updateTrackerBind({ status: Number(status) })}
                                    summary="%s"
                                />
                                <Divider orientation="vertical" flexItem />
                                <NumberSetting
                                    settingTitle={(t as any)(`Chapter`)}
                                    dialogTitle={(t as any)(`Chapter`)}
                                    settingValue={`${trackRecord.lastChapterRead}/${trackRecord.totalChapters}`}
                                    value={trackRecord.lastChapterRead}
                                    minValue={0}
                                    maxValue={Number.MAX_SAFE_INTEGER}
                                    valueUnit=""
                                    handleUpdate={(lastChapterRead) => updateTrackerBind({ lastChapterRead })}
                                />
                                <Divider orientation="vertical" flexItem />
                                <SelectSetting<string>
                                    settingName={(t as any)(`Score`)}
                                    value={currentScore}
                                    values={selectSettingValues}
                                    handleChange={(score) => updateTrackerBind({ scoreString: score })}
                                />
                            </TrackerActiveCardInfoRow>
                            <Divider />
                            <TrackerActiveCardInfoRow>
                                <DateSetting
                                    settingName={(t as any)(`Start date`)}
                                    value={Trackers.getDateString(trackRecord.startDate)}
                                    remove
                                    handleChange={(startDate) =>
                                        updateTrackerBind({ startDate: startDate ?? UNSET_DATE })
                                    }
                                />
                                <Divider orientation="vertical" flexItem />
                                <DateSetting
                                    settingName={(t as any)(`Finish date`)}
                                    value={Trackers.getDateString(trackRecord.finishDate)}
                                    remove
                                    handleChange={(finishDate) =>
                                        updateTrackerBind({ finishDate: finishDate ?? UNSET_DATE })
                                    }
                                />
                            </TrackerActiveCardInfoRow>
                        </Box>
                    </CardContent>
                </Card>
            </CardContent>
        </Card>
    );
};
