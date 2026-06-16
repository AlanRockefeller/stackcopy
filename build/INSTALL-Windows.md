# Installing Stackcopy on Windows

This guide is for installing the prebuilt, unsigned Stackcopy app on Windows.

Stackcopy is not signed with a Microsoft code-signing certificate, so Windows
SmartScreen may warn you the first time you open it. That is expected for this
release.

## What You Need

- A Windows PC.
- The `stackcopy-windows.zip` file from the Stackcopy GitHub Releases page:
  <https://github.com/AlanRockefeller/stackcopy/releases>

You do not need to install Python to use the prebuilt Windows app.

## Download Stackcopy

1. Open the Stackcopy Releases page in your web browser:
   <https://github.com/AlanRockefeller/stackcopy/releases>
2. Find the newest release at the top of the page.
3. Under **Assets**, click `stackcopy-windows.zip`.
4. Wait for the download to finish. It will usually be in your **Downloads**
   folder.

Only download Stackcopy from the official GitHub Releases page. Do not install a
copy sent by email or downloaded from an unknown website.

## Install the App

Stackcopy for Windows comes as a zip file. A zip file is a compressed folder. You
must extract it before running Stackcopy.

1. Open **File Explorer**.
2. Click **Downloads** in the left sidebar.
3. Find `stackcopy-windows.zip`.
4. Right-click `stackcopy-windows.zip`.
5. Click **Extract All...**.
6. Click **Extract**.
7. Windows will create a normal folder with the Stackcopy files inside.
8. Open the extracted folder.

You should see `Stackcopy.exe`, `StackcopyCLI.exe`, and other files or folders.
Keep them together. Do not move only `Stackcopy.exe` somewhere else, because the
app needs the other bundled files to work.

## Open Stackcopy the First Time

1. In the extracted Stackcopy folder, double-click `Stackcopy.exe`.
2. If Windows SmartScreen appears, click **More info**.
3. Click **Run anyway**.

After this first successful launch, you can open Stackcopy by double-clicking
`Stackcopy.exe`.

## Optional: Move Stackcopy Somewhere Easier

You can keep Stackcopy in Downloads, but it is usually better to move the whole
extracted folder somewhere permanent.

Good places are:

- `Documents`
- `Desktop`
- A folder you create, such as `C:\Apps\Stackcopy`

Move the whole extracted Stackcopy folder, not just `Stackcopy.exe`.

## Optional: Create a Desktop Shortcut

1. Open the extracted Stackcopy folder.
2. Right-click `Stackcopy.exe`.
3. Click **Show more options** if you are on Windows 11 and do not see all menu
   choices.
4. Click **Send to**.
5. Click **Desktop (create shortcut)**.

Use the shortcut to open Stackcopy later.

## Basic Use After Opening

1. Choose the folder you want Stackcopy to import from. This is often your camera
   card, a `DCIM` folder, or a folder copied from your camera card.
2. Check the Lightroom destination folder.
3. Check the Stack input frames folder.
4. If you are unsure, turn on **Dry run** first. Dry run previews what Stackcopy
   will do without moving files.
5. Click **Start import** when you are ready.

## Updating Stackcopy

1. Download the newest `stackcopy-windows.zip` from the Releases page.
2. Extract it with **Extract All...**.
3. Delete or rename your old Stackcopy folder.
4. Move the new extracted Stackcopy folder to the place where you keep Stackcopy.
5. If you made a desktop shortcut, delete the old shortcut and create a new one.

Updating the app does not delete your photos.

## Uninstalling Stackcopy

1. Close Stackcopy if it is open.
2. Delete the extracted Stackcopy folder.
3. Delete any desktop shortcut you created.

This removes the app. It does not delete your photos.

## Troubleshooting

### I cannot find the downloaded file

Look in your **Downloads** folder for `stackcopy-windows.zip`.

### I double-clicked `Stackcopy.exe` inside the zip and it did not work

Extract the zip first:

1. Right-click `stackcopy-windows.zip`.
2. Click **Extract All...**.
3. Click **Extract**.
4. Open the extracted folder and double-click `Stackcopy.exe` there.

### Windows does not show "Run anyway"

Click **More info** first. The **Run anyway** button appears after that.

### Windows Defender or another antivirus blocks the app

Make sure you downloaded `stackcopy-windows.zip` from the official GitHub
Releases page. If you trust that download, allow the file in your antivirus or
security app, then run `Stackcopy.exe` again.

### Stackcopy opens, but imports do not start

Make sure `StackcopyCLI.exe` is still in the same folder as `Stackcopy.exe`.
If it is missing, extract `stackcopy-windows.zip` again and run Stackcopy from
the newly extracted folder.

