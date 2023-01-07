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
use std::marker::PhantomData;
use std::sync::Arc;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek};

/// In order to open and read multiple files simultaneously, we need multiple AsyncRead + AsyncSeek streams for the
/// zip file. Note that `clone` generally doesn't work because we need independent reads and seeks across the
/// streams
pub trait GetReader<T> {
    fn get(&self) -> T;
}

// Internally, we wrap a `GetReader` and implement Clone on it to pass to the zip file library
#[pin_project]
struct Wrapper<T, U> {
    #[pin]
    inner: T,

    // Used to get new readers when we clone
    factory: Arc<U>,
}

impl<T, U> Wrapper<T, U>
where
    U: GetReader<T>,
{
    fn new(factory: Arc<U>) -> Self {
        Self {
            inner: factory.get(),
            factory,
        }
    }
}

impl<T, U> Clone for Wrapper<T, U>
where
    U: GetReader<T>,
{
    fn clone(&self) -> Self {
        Self::new(self.factory.clone())
    }
}

// Pass through AsyncRead
impl<T, U> AsyncRead for Wrapper<T, U>
where
    T: AsyncRead,
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
impl<T, U> AsyncSeek for Wrapper<T, U>
where
    T: AsyncSeek,
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

pub struct ZipFS<'a, R, U>
where
    R: AsyncRead + AsyncSeek + Unpin,
    U: GetReader<R>,
{
    zip: ZipFileReader<Wrapper<R, U>>,
    entries: Vec<ZipEntry>,
    pd: PhantomData<&'a ()>,
}

impl<'a, R, U> ZipFS<'a, R, U>
where
    R: 'a + AsyncRead + AsyncSeek + Unpin + Sync,
    U: GetReader<R>,
{
    pub async fn new(factory: U) -> ZipFS<'a, R, U> {
        let wrapper = Wrapper::new(Arc::new(factory));

        let zip = ZipFileReader::new(wrapper).await.unwrap();

        // This makes a copy of each entry, but that's probably ok
        let entries = zip
            .file()
            .entries()
            .into_iter()
            .map(|e| e.entry().clone())
            .collect();

        Self {
            zip,
            entries,
            pd: PhantomData,
        }
    }
}

// We can't use `ZipEntryReader` as our file type directly because we need to implement
// the `ReadableFile` trait for it. The orphan rules prevent doing this so we need to
// wrap `ZipEntryReader` in our own type.
#[pin_project]
pub struct File<R, U>
where
    R: AsyncRead + Unpin + 'static,
    U: 'static,
{
    entry: ZipEntry,

    #[pin]
    inner: ZipEntryReader<'static, Wrapper<R, U>>,
}

// Pass reads through
impl<R, U> AsyncRead for File<R, U>
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

#[async_trait]
impl<R, U> ReadableFile for File<R, U>
where
    R: AsyncRead + Unpin + Sync,
    U: Sync + Send,
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

impl<'a, R, U> HasFileType for ZipFS<'a, R, U>
where
    R: 'a + AsyncRead + AsyncSeek + Unpin + Sync + 'static,
    U: 'a + GetReader<R> + 'static,
{
    type FileType = File<R, U>;
}

#[async_trait]
impl<'a, R, U> ReadableFileSystem for ZipFS<'a, R, U>
where
    R: 'a + AsyncRead + AsyncSeek + Unpin + Sync + Send + 'static,
    U: 'a + Sync + Send + GetReader<R> + 'static,
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

impl<'b, R, U> ReadDirPoller<ZipFS<'b, R, U>> for ZipReadDirPoller
where
    R: 'b + AsyncRead + AsyncSeek + Unpin + Sync + Send + 'static,
    U: 'b + Sync + Send + GetReader<R> + 'static,
{
    fn poll_next_entry<'a>(
        &mut self,
        _cx: &mut std::task::Context<'_>,
        fs: &'a ZipFS<'b, R, U>,
    ) -> std::task::Poll<std::io::Result<Option<lunchbox::types::DirEntry<'a, ZipFS<'b, R, U>>>>>
    {
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
