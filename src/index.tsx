import React from "react";

import LibraryCapture from "./components/LibraryCapture";
import LibraryEditor from "./components/LibraryEditor";

export default LibraryEditor;

export { LibraryCapture, LibraryEditor };

export const version = "1.1.0";

export const metadata = {
  name: "BrainDriveLibraryPlugin",
  description:
    "Library editor and capture modules for browsing, editing, and routing BrainDrive library updates",
  version,
  author: "BrainDrive",
};
