## Sam-ZFS-unlocker

This is a simple library that has some functions to lock/unlock datasets in your machine. This isn't intended to be run or even compiled as root.

## Usage with sudo

The way to use this is by creating a special user and granting them special `sudo` permissions to run the given commands. The functions that require visudo to be edited for the given user are specified in the documentation of every function. A subset of those are "mount", "unmount", "load-key" and "unload-key". More may be added.
