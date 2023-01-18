use async_trait::async_trait;
use async_zip::read::ZipEntryReader;
use async_zip::read::seek::ZipFileReader;
use async_zip::{ZipEntry, AttributeCompatibility};
use lunchbox::path::PathBuf;
use lunchbox::types::{
    DirEntry, FileType, HasFileType, Metadata, Permissions, ReadDir, ReadDirPoller, ReadableFile, MaybeSend, MaybeSync,
};
use lunchbox::{types::PathType, ReadableFileSystem};
use pin_project::pin_project;
use std::io::{ErrorKind, Result};
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek};

/// In order to open and read multiple files simultaneously, we need multiple AsyncRead + AsyncSeek streams for the
/// zip file. Note that `clone` generally doesn't work because we need independent reads and seeks across the
/// streams
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait GetReader {
    type R: AsyncRead + AsyncSeek + Unpin;

    async fn get(&self) -> Self::R;
}

pub struct ZipFS<T>
where
    T: GetReader,
{
    factory: T,
    zip_info: async_zip::ZipFile,
    entries: Vec<ZipEntry>,
}

impl<T> ZipFS<T>
where
    T: GetReader,
{
    pub async fn new(factory: T) -> ZipFS<T> {
        let zip = ZipFileReader::new(factory.get().await).await.unwrap();

        // This makes a copy of each entry, but that's probably ok
        let entries = zip
            .file()
            .entries()
            .into_iter()
            .map(|e| e.entry().clone())
            .collect();

        Self {
            factory,
            zip_info: zip.file().clone(),
            entries,
        }
    }
}

// We can't use `ZipEntryReader` as our file type directly because we need to implement
// the `ReadableFile` trait for it. The orphan rules prevent doing this so we need to
// wrap `ZipEntryReader` in our own type.
#[pin_project]
pub struct File<'a, R> {
    entry: ZipEntry,

    #[pin]
    inner: ZipEntryReader<'a, R>,
}

// Pass reads through
impl<'a, R> AsyncRead for File<'a, R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<'a, R> ReadableFile for File<'a, R>
where
    R: AsyncRead + Unpin + MaybeSync,
{
    async fn metadata(&self) -> Result<Metadata> {
        let entry = &self.entry;

        // A file is considered a directory if it ends in /
        // This is what python does as well
        // https://github.com/python/cpython/blob/820ef62833bd2d84a141adedd9a05998595d6b6d/Lib/zipfile.py#L528
        let is_dir = entry.filename().ends_with("/");

        // We don't support symlinks yet
        let is_symlink = false;

        // Since we don't support symlinks, an entry is a file if it's not a dir
        let is_file = !is_dir;

        // We don't have accessed or created
        let accessed = Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "ZipFS does not support `accessed`",
        ));
        let created = Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "ZipFS does not support `created`",
        ));

        // Get the modified time from the zipfile
        let modified = Ok((entry.last_modification_date().as_chrono().single().unwrap()).into());

        // Finally, len is the uncompressed size
        let len = entry.uncompressed_size();

        let file_type = FileType::new(is_dir, is_file, is_symlink);
        let permissions = Permissions::new(true);
        let out = Metadata::new(
            accessed,
            created,
            modified,
            file_type,
            len as _,
            permissions,
        );

        Ok(out)
    }

    async fn try_clone(&self) -> Result<Self> {
        // We don't support try_clone for now because it needs to share read and seek position with self
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "ZipFS does not currently support try_clone",
        ))
    }
}

impl<T> HasFileType for ZipFS<T>
where
    T: GetReader + 'static,
{
    type FileType = File<'static, T::R>;
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<T> ReadableFileSystem for ZipFS<T>
where
    T: GetReader + 'static + MaybeSync + MaybeSend,
    T::R: MaybeSync + MaybeSend,
{
    async fn open(&self, path: impl PathType) -> Result<Self::FileType>
    where
        Self::FileType: ReadableFile,
    {
        // Find the file we want to open
        let path = self.canonicalize(path).await?;

        let found = self
            .entries
            .iter()
            .enumerate()
            .find(|(_, entry)| path == entry.filename());

        let (entry_idx, entry) = match found {
            Some(item) => item,
            None => return Err(std::io::Error::new(ErrorKind::NotFound, "File not found")),
        };

        // Create a new ZipFileReader
        let zip = ZipFileReader::from_parts(self.factory.get().await, self.zip_info.clone());

        // Turn it into an entry reader
        let inner = zip.into_entry(entry_idx).await.unwrap();

        Ok(File {
            inner,
            entry: entry.clone(),
        })
    }

    async fn canonicalize(&self, path: impl PathType) -> Result<PathBuf> {
        // We don't currently support symlinks so canonicalize and normailze are the same
        Ok(path_clean::clean(path.as_ref().as_str()).into())
    }

    async fn metadata(&self, path: impl PathType) -> Result<Metadata> {
        self.open(path).await?.metadata().await
    }

    async fn read(&self, path: impl PathType) -> Result<Vec<u8>> {
        let mut v = Vec::new();
        let _ = self.open(path).await?.read_to_end(&mut v).await?;
        Ok(v)
    }

    type ReadDirPollerType = ZipReadDirPoller;

    async fn read_dir(
        &self,
        path: impl PathType,
    ) -> Result<ReadDir<Self::ReadDirPollerType, Self>> {
        // Find all files within that directory
        let base_path = self.canonicalize(path).await?;
        let filtered = self
            .entries
            .iter()
            .filter_map(|entry| {
                let file_path = PathBuf::from(entry.filename());
                if file_path.starts_with(&base_path) {
                    Some(file_path)
                } else {
                    None
                }
            })
            .collect();

        let poller = ZipReadDirPoller { entries: filtered };

        Ok(ReadDir::new(poller, self))
    }

    async fn read_link(&self, _path: impl PathType) -> Result<PathBuf> {
        // We don't support symlinks for now
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "ZipFS does not currently support symlinks",
        ))
    }

    async fn read_to_string(&self, path: impl PathType) -> Result<String> {
        let mut s = String::new();
        let _ = self.open(path).await?.read_to_string(&mut s).await?;
        Ok(s)
    }

    async fn symlink_metadata(&self, path: impl PathType) -> Result<Metadata> {
        // We don't support symlinks yet so just pass through to metadata
        self.metadata(path).await
    }
}

pub struct ZipReadDirPoller {
    entries: Vec<PathBuf>,
}

impl<T> ReadDirPoller<ZipFS<T>> for ZipReadDirPoller
where
    T: GetReader + 'static + MaybeSync + MaybeSend,
    T::R: MaybeSync + MaybeSend,
{
    fn poll_next_entry<'a>(
        &mut self,
        _cx: &mut std::task::Context<'_>,
        fs: &'a ZipFS<T>,
    ) -> std::task::Poll<std::io::Result<Option<lunchbox::types::DirEntry<'a, ZipFS<T>>>>> {
        match self.entries.pop() {
            Some(path) => {
                let file_name = path.file_name().unwrap().into();

                Poll::Ready(Ok(Some(DirEntry::new(fs, file_name, path))))
            }
            // We're done reading entries
            None => Poll::Ready(Ok(None)),
        }
    }
}
