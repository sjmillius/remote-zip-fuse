# Mount a remote zipfile as a fuse vfs.

Uses http range requests to not have to download the entire zip file, but only the required portions.

## Usage

```bash
python mount_remote_zip_fuse.py --url=... --mountpoint=... --verbose
```