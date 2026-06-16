# Installing Stackcopy on macOS

This guide is for installing the prebuilt, unsigned Stackcopy app on a Mac.

Stackcopy is not signed with an Apple developer certificate, so macOS may warn
you the first time you open it. That is expected for this release.

## What You Need

- A Mac running macOS.
- The `Stackcopy.dmg` file from the Stackcopy GitHub Releases page:
  <https://github.com/AlanRockefeller/stackcopy/releases>
- Your Mac user password, if macOS asks for it.

## Download Stackcopy

1. Open the Stackcopy Releases page in your web browser:
   <https://github.com/AlanRockefeller/stackcopy/releases>
2. Find the newest release at the top of the page.
3. Under **Assets**, click `Stackcopy.dmg`.
4. Wait for the download to finish. It will usually be in your **Downloads**
   folder.

Only download Stackcopy from the official GitHub Releases page. Do not install a
copy sent by email or downloaded from an unknown website.

## Install the App

1. Open **Finder**.
2. Click **Downloads** in the sidebar.
3. Double-click `Stackcopy.dmg`.
4. A small Stackcopy disk window should open.
5. Drag **Stackcopy** into the **Applications** folder.
6. Wait for the copy to finish.
7. In Finder, click the eject button next to the Stackcopy disk image.

Stackcopy is now installed in your **Applications** folder.

## Open Stackcopy the First Time

Do not open the app by double-clicking it the first time. macOS is stricter with
unsigned apps, so use the special first-open method below.

1. Open **Finder**.
2. Click **Applications** in the sidebar.
3. Find **Stackcopy**.
4. Hold the **Control** key and click **Stackcopy**. You can also right-click it
   if your mouse or trackpad is set up for right-click.
5. Click **Open**.
6. macOS will show a warning because Stackcopy is unsigned.
7. Click **Open** again.

After this first successful launch, you can open Stackcopy normally by
double-clicking it.

## If macOS Blocks the App Completely

Some macOS versions show a message like "Apple could not verify Stackcopy is free
of malware" and may not show an **Open** button at first.

Try this:

1. Open **System Settings**.
2. Click **Privacy & Security**.
3. Scroll down to the **Security** section.
4. Look for a message saying Stackcopy was blocked.
5. Click **Open Anyway**.
6. Enter your Mac password or use Touch ID if macOS asks.
7. Go back to **Applications** and open **Stackcopy** again.
8. Click **Open** if macOS asks one more time.

## Basic Use After Opening

1. Choose the folder you want Stackcopy to import from. This is often your camera
   card, a `DCIM` folder, or a folder copied from your camera card.
2. Check the Lightroom destination folder.
3. Check the Stack input frames folder.
4. If you are unsure, turn on **Dry run** first. Dry run previews what Stackcopy
   will do without moving files.
5. Click **Start import** when you are ready.

## Updating Stackcopy

1. Download the newest `Stackcopy.dmg` from the Releases page.
2. Open the new `.dmg`.
3. Drag **Stackcopy** into **Applications** again.
4. If macOS asks whether to replace the existing app, click **Replace**.
5. Open Stackcopy using the first-open steps again if macOS shows the unsigned
   app warning.

## Uninstalling Stackcopy

1. Open **Finder**.
2. Click **Applications**.
3. Drag **Stackcopy** to the Trash.
4. Empty the Trash if you want to remove it completely.

This removes the app. It does not delete your photos.

## Troubleshooting

### I cannot find the downloaded file

Look in your **Downloads** folder for `Stackcopy.dmg`.

### The app says it is damaged

Delete `Stackcopy.dmg`, download it again from the official Releases page, and
repeat the install steps.

### Nothing happens when I double-click the `.dmg`

Try right-clicking `Stackcopy.dmg` and choosing **Open**. If it still does not
open, download the file again.

### Stackcopy cannot access my camera card or folders

macOS may ask for permission to access removable volumes, Desktop, Documents, or
Downloads. Click **Allow** when asked. If you clicked **Don't Allow**, open
**System Settings** > **Privacy & Security** > **Files and Folders** and allow
Stackcopy to access the folders you want to use.

