/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeSanitize from 'rehype-sanitize';
import Box from '@mui/material/Box';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import Icon from '@mui/material/Icon';
import ImageOutlinedIcon from '@mui/icons-material/ImageOutlined';
import Stack from '@mui/material/Stack';
import { useTheme } from '@mui/material/styles';
import Typography from '@mui/material/Typography';

type MarkdownRendererProps = {
    source?: string | null;
    className?: string;
    loadImages?: boolean; // when false, inline images are replaced with a placeholder icon
    maxHeight?: string | number;
};

// A focused sanitize schema that explicitly allows the tags/attributes we want.
const sanitizeSchema: any = {
    tagNames: [
        'p',
        'h1',
        'h2',
        'h3',
        'h4',
        'h5',
        'h6',
        'blockquote',
        'ul',
        'ol',
        'li',
        'pre',
        'code',
        'hr',
        'br',
        'table',
        'thead',
        'tbody',
        'tr',
        'th',
        'td',
        'a',
        'strong',
        'em',
        'img',
    ],
    attributes: {
        a: ['href', 'title'],
        img: ['src', 'alt', 'title'],
        '*': ['className', 'align'],
    },
    protocols: {
        href: ['http', 'https', 'mailto', 'tel'],
        src: ['http', 'https', 'data'],
    },
};

export const MarkdownRenderer: React.FC<MarkdownRendererProps> = ({
    source,
    className,
    loadImages = true,
    maxHeight,
}) => {
    const theme = useTheme();

    if (!source) return null;

    // rehypePlugins: cast to satisfy react-markdown/unified typings.
    const rehypePlugins = [[rehypeSanitize, sanitizeSchema]] as unknown as any;

    return (
        <Box
            className={className}
            sx={{
                color: theme.palette.text.primary,
                // tighter spacing than browser defaults to avoid large gaps
                '& h1, & h2, & h3, & h4, & h5, & h6': {
                    marginTop: theme.spacing(1),
                    marginBottom: theme.spacing(0.5),
                },
                '& p': {
                    marginTop: 0,
                    marginBottom: theme.spacing(1),
                },
                '& a': {
                    color: theme.palette.primary.main,
                    textDecoration: 'none',
                    '&:hover': { textDecoration: 'underline' },
                },
                '& pre': {
                    background: theme.palette.action.hover,
                    padding: theme.spacing(1),
                    borderRadius: 1,
                    overflow: 'auto',
                    margin: theme.spacing(1, 0),
                },
                '& code': {
                    fontFamily: 'monospace',
                    background: theme.palette.action.hover,
                    padding: '2px 4px',
                    borderRadius: '4px',
                    fontSize: '0.95em',
                },
                '& table': {
                    marginTop: theme.spacing(1),
                    marginBottom: theme.spacing(1),
                    width: '100%',
                    borderCollapse: 'collapse',
                },
                '& th, & td': {
                    border: `1px solid ${theme.palette.divider}`,
                    padding: theme.spacing(0.5),
                },
                '& img': {
                    maxWidth: '100%',
                    display: 'block',
                },
                // Use border-top for hr so it stays visible across themes
                '& hr': {
                    border: 0,
                    borderTop: `1px solid ${theme.palette.divider}`,
                    height: 0,
                    margin: theme.spacing(1, 0),
                },
                maxHeight: maxHeight ?? 'auto',
                overflow: maxHeight ? 'auto' : undefined,
            }}
        >
            <ReactMarkdown
                remarkPlugins={[remarkGfm]}
                rehypePlugins={rehypePlugins}
                components={{
                    // paragraphs: wrap in MUI Typography for consistent spacing
                    p: ({ children, ...rest }: any) => (
                        <Typography component="div" variant="body2" {...rest}>
                            {children}
                        </Typography>
                    ),
                    // links: open external links in new tab and render children explicitly
                    a: ({ children, href, ...rest }: any) => {
                        const isInternal = href && (href.startsWith('/') || href.startsWith(window.location.origin));
                        // eslint-disable-next-line react/jsx-no-target-blank
                        return (
                            <a
                                {...rest}
                                href={href}
                                target={isInternal ? undefined : '_blank'}
                                rel={isInternal ? undefined : 'noopener noreferrer'}
                            >
                                {children}
                            </a>
                        );
                    },
                    // images: either render the image or a placeholder icon
                    img: ({ src, alt, title, ...rest }: any) => {
                        if (!loadImages) {
                            return (
                                <Stack direction="row" alignItems="center" gap={1}>
                                    <Icon fontSize="small" color="action">
                                        <ImageOutlinedIcon />
                                    </Icon>
                                    <Typography variant="body2" color="text.secondary">
                                        {alt ?? ''}
                                    </Typography>
                                </Stack>
                            );
                        }
                        return (
                            // eslint-disable-next-line jsx-a11y/img-redundant-alt
                            <img
                                {...rest}
                                src={src}
                                alt={alt ?? ''}
                                title={title}
                                loading="lazy"
                                style={{ maxWidth: '100%', borderRadius: 4 }}
                            />
                        );
                    },
                    // unordered lists: rely on browser markup but wrap in Box for spacing
                    ul: ({ children, ...rest }: any) => (
                        <Box component="ul" sx={{ pl: 3, mb: 1 }} {...rest}>
                            {children}
                        </Box>
                    ),
                    // ordered lists
                    ol: ({ children, ...rest }: any) => (
                        <Box component="ol" sx={{ pl: 3, mb: 1 }} {...rest}>
                            {children}
                        </Box>
                    ),
                    // list item
                    li: ({ children }: any) => <Box component="li">{children}</Box>,
                    // code blocks & inline code
                    code: ({ inline, children }: any) => {
                        if (inline) {
                            return (
                                <Box
                                    component="code"
                                    sx={{ fontFamily: 'monospace', bgcolor: 'action.hover', px: 0.5, py: 0 }}
                                >
                                    {children}
                                </Box>
                            );
                        }
                        // For block code, react-markdown wraps with <pre><code> so we only render children here.
                        return (
                            <Box
                                component="pre"
                                sx={{ whiteSpace: 'pre', p: 1, bgcolor: 'action.hover', borderRadius: 1 }}
                            >
                                <Box component="code" sx={{ fontFamily: 'monospace' }}>
                                    {children}
                                </Box>
                            </Box>
                        );
                    },
                    // tables: wrap in Paper so they look consistent with the app
                    table: ({ children, ...rest }: any) => (
                        <Paper variant="outlined" sx={{ width: '100%', overflowX: 'auto', my: 1 }} {...rest}>
                            <Box component="div" sx={{ px: 1 }}>
                                {children}
                            </Box>
                        </Paper>
                    ),
                    thead: ({ children }: any) => <TableHead>{children}</TableHead>,
                    tbody: ({ children }: any) => <TableBody>{children}</TableBody>,
                    tr: ({ children }: any) => <TableRow>{children}</TableRow>,
                    th: ({ children }: any) => (
                        <TableCell component="th" sx={{ fontWeight: 'bold' }}>
                            {children}
                        </TableCell>
                    ),
                    td: ({ children }: any) => <TableCell>{children}</TableCell>,
                    // horizontal rule: use visible border-top
                    hr: () => (
                        <Box
                            component="hr"
                            sx={{
                                border: 0,
                                borderTop: `1px solid ${theme.palette.divider}`,
                                height: 0,
                                my: 1,
                            }}
                        />
                    ),
                    // blockquote: render with an accent left border
                    blockquote: ({ children }: any) => (
                        <Box sx={{ borderLeft: `4px solid ${theme.palette.action.selected}`, pl: 2, my: 1 }}>
                            <Typography variant="body2" sx={{ fontStyle: 'italic' }}>
                                {children}
                            </Typography>
                        </Box>
                    ),
                }}
            >
                {source}
            </ReactMarkdown>
        </Box>
    );
};
