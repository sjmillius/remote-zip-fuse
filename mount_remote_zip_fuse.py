'''Expose a remote zip file as fuse vfs.'''

from absl import app
from absl import flags
import datetime
import errno
import fuse
import stat
import urlfile
import zipfile

_FLAG_REMOTE_URL = flags.DEFINE_string(name='url',
                                       default=None,
                                       help='Url of the remote zip file.',
                                       required=True)
_FLAG_MOUNTPOINT = flags.DEFINE_string(name='mountpoint',
                                       default=None,
                                       help='The mountpoint',
                                       required=True)
_FLAG_VERBOSE = flags.DEFINE_bool(
    name='verbose',
    help='Whether to show download progress indicators and verbose information.',
    default=False)


class ZipFs(fuse.Operations):

  def __init__(self, zip_archive: zipfile.ZipFile):
    self.files = {}
    self.epoch = datetime.datetime.utcfromtimestamp(0)
    self.zip_archive = zip_archive
    self.path = zipfile.Path(root=zip_archive)

  def unix_time_millis(self, x):
    dt = datetime.datetime(year=x[0],
                           month=x[1],
                           day=x[2],
                           hour=x[3],
                           minute=x[4],
                           second=x[5])
    return (dt - self.epoch).total_seconds()

  def getattr(self, path, fh=None):
    if path == '/':
      return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2, st_size=1)

    zip_path = self.path.joinpath(path[1:])
    try:
      if zip_path.is_dir():
        return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
      else:
        info = self.zip_archive.getinfo(path[1:])
        return dict(st_mode=(stat.S_IFREG | 0o777),
                    st_nlink=1,
                    st_size=info.file_size,
                    st_mtime=self.unix_time_millis(info.date_time))
    except KeyError:
      raise fuse.FuseOSError(errno.ENOENT)

  def readdir(self, path, fh):
    zip_path = self.path
    if path != '/':
      zip_path = zip_path.joinpath(path[1:])
    for f in zip_path.iterdir():
      yield f.name

  def open(self, path, flags):
    return 0

  def read(self, path, length, offset, fh):
    zip_path = self.path.joinpath(path[1:])
    if zip_path.is_dir() or not zip_path.exists():
      raise fuse.FuseOSError(errno.ENOENT)
    with self.zip_archive.open(path[1:]) as f:
      f.seek(offset)
      return f.read(length)

  def flush(self, path, fh):
    return 0

  def write(self, path, buf, offset, fh):
    raise fuse.FuseOSError(errno.EROFS)

  def truncate(self, path, length, fh=None):
    raise fuse.FuseOSError(errno.EROFS)

  def create(self, path, mode, fi=None):
    raise fuse.FuseOSError(errno.EROFS)

  def mkdir(self, path, mode):
    raise fuse.FuseOSError(errno.EROFS)

  def unlink(self, path):
    raise fuse.FuseOSError(errno.EROFS)

  def rename(self, old, new):
    raise fuse.FuseOSError(errno.EROFS)

  def rmdir(self, path):
    raise fuse.FuseOSError(errno.EROFS)


def main(_):
  fuse.FUSE(ZipFs(zip_archive=zipfile.ZipFile(
      urlfile.BufferedUrlFile(url=_FLAG_REMOTE_URL.value,
                              verbose=_FLAG_VERBOSE.value))),
            _FLAG_MOUNTPOINT.value,
            nothreads=True,
            foreground=True)


if __name__ == '__main__':
  app.run(main)
