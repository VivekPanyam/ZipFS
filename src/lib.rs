use async_trait::async_trait;
use async_zip::read::seek::{ZipEntryReader, ZipFileReader};
use async_zip::ZipEntry;
use lunchbox::path::PathBuf;
use lunchbox::types::{
    DirEntry, FileType, HasFileType, Metadata, Permissions, ReadDir, ReadDirPoller, ReadableFile,
};
use lunchbox::{types::PathType, ReadableFileSystem};
use pin_project::pin_project;
use std::io::{ErrorKind, Result};
use std::sync::Arc;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek};

/// In order to open and read multiple files simultaneously, we need multiple AsyncRead + AsyncSeek streams for the
/// zip file. Note that `clone` generally doesn't work because we need independent reads and seeks across the
/// streams
pub trait GetReader {
    type R: AsyncRead + AsyncSeek + Unpin;

    fn get(&self) -> Self::R;
}

// Internally, we wrap a `GetReader` and implement Clone on it to pass to the zip file library
#[pin_project]
struct Wrapper<T>
where
    T: GetReader,
{
    #[pin]
    inner: T::R,

    // Used to get new readers when we clone
    factory: Arc<T>,
}

impl<T> Wrapper<T>
where
    T: GetReader,
{
    fn new(factory: Arc<T>) -> Self {
        Self {
            inner: factory.get(),
            factory,
        }
    }
}

impl<T> Clone for Wrapper<T>
where
    T: GetReader,
{
    fn clone(&self) -> Self {
        Self::new(self.factory.clone())
    }
}

// Pass through AsyncRead
impl<T> AsyncRead for Wrapper<T>
where
    T: GetReader,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
    }
}

// Pass through AsyncSeek
impl<T> AsyncSeek for Wrapper<T>
where
    T: GetReader,
{
    fn start_seek(
        self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        self.project().inner.start_seek(position)
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        self.project().inner.poll_complete(cx)
    }
}

pub struct ZipFS<T>
where
    T: GetReader,
{
    zip: ZipFileReader<Wrapper<T>>,
    entries: Vec<ZipEntry>,
}

impl<T> ZipFS<T>
where
    T: GetReader,
{
    pub async fn new(factory: T) -> ZipFS<T> {
        let wrapper = Wrapper::new(Arc::new(factory));

        let zip = ZipFileReader::new(wrapper).await.unwrap();

        // This makes a copy of each entry, but that's probably ok
        let entries = zip
            .file()
            .entries()
            .into_iter()
            .map(|e| e.entry().clone())
            .collect();

        Self { zip, entries }
    }
}

// We can't use `ZipEntryReader` as our file type directly because we need to implement
// the `ReadableFile` trait for it. The orphan rules prevent doing this so we need to
// wrap `ZipEntryReader` in our own type.
#[pin_project]
pub struct File<'a, T>
where
    T: GetReader,
{
    entry: ZipEntry,

    #[pin]
    inner: ZipEntryReader<'a, Wrapper<T>>,
}

// Pass reads through
impl<'a, T> AsyncRead for File<'a, T>
where
    T: GetReader,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
    }
}

#[async_trait]
impl<'a, T> ReadableFile for File<'a, T>
where
    T: GetReader + Sync + Send,
    T::R: Sync,
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
    type FileType = File<'static, T>;
}

#[async_trait]
impl<T> ReadableFileSystem for ZipFS<T>
where
    T: GetReader + 'static + Sync + Send,
    T::R: Sync + Send,
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

        // The underlying reader needs to be clonable (with independent seeking) in order to support opening
        // and reading multiple files within the zip at the same time
        // We clone the ZipFileReader here because that'll clone the wrapper and get a new underlying reader (see GetReader)
        // We turn that into an entry reader and return that as a file
        let inner = self.zip.clone().into_entry(entry_idx).await.unwrap();

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
    T: GetReader + 'static + Sync + Send,
    T::R: Sync + Send,
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
