# BrainDrive Library Plugin

A BrainDrive frontend plugin that adds a **Library Editor** module for browsing and editing user-scoped library files.

## Features

- Safe folder navigation rooted to the user library scope
- Markdown preview with edit/save workflow
- Plain text editing support for `.txt`, `.json`, `.yaml`, and `.yml`
- Theme-aware UI using BrainDrive theme bridge values
- Mobile-responsive panel layout

## Build

```bash
npm install
npm run build:local
```

Or for a release bundle:

```bash
npm run build:release
```

The webpack Module Federation bundle is produced at `dist/remoteEntry.js`.
