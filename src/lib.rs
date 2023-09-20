// Copyright 2023 Vivek Panyam
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![doc = include_str!("../README.md")]

use async_trait::async_trait;
use async_zip::base::read::seek::ZipFileReader;
use async_zip::base::read::{WithoutEntry, ZipEntryReader};
use async_zip::{AttributeCompatibility, ZipEntry};
use lunchbox::path::PathBuf;
use lunchbox::types::{
    DirEntry, FileType, HasFileType, MaybeSend, MaybeSync, Metadata, Permissions, ReadDir,
    ReadDirPoller, ReadableFile,
};
use lunchbox::{types::PathType, ReadableFileSystem};
use pin_project::pin_project;
use std::collections::HashSet;
use std::future::Future;
use std::io::{ErrorKind, Result};
use std::ops::Deref;
use std::pin::Pin;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

/// In order to open and read multiple files simultaneously, we need multiple AsyncRead + AsyncSeek streams for the
/// zip file. Note that `clone` generally doesn't work because we need independent reads and seeks across the
/// streams
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait GetReader {
    type R: AsyncRead + AsyncSeek + Unpin;

    async fn get(&self) -> Self::R;
}

// Allow passing in local paths to the ZipFS constructor
#[cfg(not(target_family = "wasm"))]
#[async_trait]
impl GetReader for std::path::PathBuf {
    type R = tokio::fs::File;

    async fn get(&self) -> Self::R {
        tokio::fs::File::open(&self).await.unwrap()
    }
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
        let zip = ZipFileReader::new(factory.get().await.compat())
            .await
            .unwrap();

        // This makes a copy of each entry, but that's probably ok
        let entries = zip
            .file()
            .entries()
            .into_iter()
            .map(|e| e.deref().clone())
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
pub struct File<R> {
    entry: ZipEntry,

    #[cfg(not(target_family = "wasm"))]
    loader: Pin<Box<dyn Future<Output = R> + Send + Sync>>,

    #[cfg(target_family = "wasm")]
    loader: Pin<Box<dyn Future<Output = R>>>,

    #[pin]
    inner: Option<R>,
}

// Pass reads through
impl<R> AsyncRead for File<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Lazy load the reader
        if self.inner.is_none() {
            match self.loader.as_mut().poll(cx) {
                Poll::Ready(r) => self.inner = Some(r),
                Poll::Pending => return Poll::Pending,
            }
        }

        self.project()
            .inner
            .as_pin_mut()
            .unwrap()
            .poll_read(cx, buf)
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<R> ReadableFile for File<R>
where
    R: AsyncRead + Unpin + MaybeSync,
{
    async fn metadata(&self) -> Result<Metadata> {
        let entry = &self.entry;

        // A file is considered a directory if it ends in /
        // This is what python does as well
        // https://github.com/python/cpython/blob/820ef62833bd2d84a141adedd9a05998595d6b6d/Lib/zipfile.py#L528
        let is_dir = entry.filename().as_str().unwrap().ends_with("/");

        let is_symlink = if entry.attribute_compatibility() == AttributeCompatibility::Unix {
            // The high two bytes of the external_file_attribute store st_mode
            // https://unix.stackexchange.com/a/14727
            let mode = entry.external_file_attribute() >> 16;

            const S_IFLNK: u32 = 40960;
            mode & S_IFLNK == S_IFLNK
        } else {
            false
        };

        // An entry is a file if it's not a dir or a symlink
        let is_file = !is_dir && !is_symlink;

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
        let modified = match entry.last_modification_date().as_chrono().single() {
            Some(v) => Ok(v.into()),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No modification time available",
            )),
        };

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
    type FileType = File<Compat<ZipEntryReader<'static, Compat<T::R>, WithoutEntry>>>;
}

impl<T> ZipFS<T>
where
    T: GetReader + 'static + MaybeSync + MaybeSend,
    T::R: MaybeSync + MaybeSend,
{
    /// Opens a file without following symlinks
    /// This means if `path` is a symlink, the returned file will be the symlink itself
    async fn open_no_follow_symlink(
        &self,
        path: impl PathType,
    ) -> Result<<ZipFS<T> as HasFileType>::FileType>
    where
        <ZipFS<T> as HasFileType>::FileType: ReadableFile,
    {
        // Normalize the path
        let path: PathBuf = path_clean::clean(path.as_ref().as_str()).into();

        let found = self
            .entries
            .iter()
            .enumerate()
            .find(|(_, entry)| path == entry.filename().as_str().unwrap());

        let (entry_idx, entry) = match found {
            Some(item) => item,
            None => {
                return Err(std::io::Error::new(
                    ErrorKind::NotFound,
                    format!("File not found in zipfile: {path}"),
                ))
            }
        };

        // Create a new ZipFileReader
        let zip =
            ZipFileReader::from_raw_parts(self.factory.get().await.compat(), self.zip_info.clone());

        Ok(File {
            inner: None,
            loader: Box::pin(async move {
                // Turn it into an entry reader
                zip.into_entry(entry_idx).await.unwrap().compat()
            }),
            entry: entry.clone(),
        })
    }
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

        // Canonicalize follows the entire symlink chain so we can just call through to this
        self.open_no_follow_symlink(&path).await
    }

    async fn canonicalize(&self, path: impl PathType) -> Result<PathBuf> {
        // We don't currently support directory symlinks. This means we only need to worry about the last
        // component being a symlink. This means we can just open the file at `path` and keep following symlinks
        // until we hit a target that isn't a symlink (or doesn't exist)
        let mut path: PathBuf = path.as_ref().to_owned();
        let mut visited = HashSet::new();
        loop {
            // Normalize the path
            path = path_clean::clean(path.as_str()).into();

            // Return an error if we've already visited this path
            if visited.contains(&path) {
                return Err(std::io::Error::new(
                    // TODO: use ErrorKind::FilesystemLoop once stable
                    std::io::ErrorKind::Other,
                    "Found symlink loop",
                ));
            }

            // Track that we've seen the path
            visited.insert(path.clone());

            // Open the file and check if it's a symlink
            let f = self.open_no_follow_symlink(&path).await;

            // If the file doesn't exist, we can just return the path
            if f.is_err() {
                return Ok(path);
            }

            // Otherwise, grab metadata and check if it's a symlink
            let mut f = f.unwrap();
            let metadata = f.metadata().await?;
            if metadata.is_symlink() {
                // Read the target path and continue looping
                let mut target = String::new();
                f.read_to_string(&mut target).await?;

                // Relative to the parent dir
                path = path.parent().unwrap().join(target);
            } else {
                return Ok(path);
            }
        }
    }

    async fn metadata(&self, path: impl PathType) -> Result<Metadata> {
        // Note: `open` follows the symlink chain (if any)
        self.open(path).await?.metadata().await
    }

    async fn read(&self, path: impl PathType) -> Result<Vec<u8>> {
        // Note: `open` follows the symlink chain (if any)
        let mut f = self.open(path).await?;

        // Get the length and read the whole file
        let len = f.metadata().await?.len();
        let mut v = vec![0; len as _];
        let _ = f.read_exact(&mut v).await?;
        Ok(v)
    }

    type ReadDirPollerType = ZipReadDirPoller;

    async fn read_dir(
        &self,
        path: impl PathType,
    ) -> Result<ReadDir<Self::ReadDirPollerType, Self>> {
        // We don't support directory symlinks yet so we don't have to do anything special here
        // Find all files within that directory
        let base_path = self.canonicalize(path).await?;
        let filtered = self
            .entries
            .iter()
            .filter_map(|entry| {
                let file_path = PathBuf::from(entry.filename().as_str().unwrap());
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

    async fn read_link(&self, path: impl PathType) -> Result<PathBuf> {
        // Open the file and check if it's a symlink
        let path = path.as_ref();
        let mut f = self.open_no_follow_symlink(path).await?;
        let metadata = f.metadata().await?;
        if metadata.is_symlink() {
            // Read the target path
            let mut target = String::new();
            f.read_to_string(&mut target).await?;

            // We only support symlinks that are relative paths
            if target.starts_with("/") {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Tried to read a symlink with an absolute target, but these are not supported in ZipFS",
                ))
            } else {
                // The symlink target is relative to the parent dir of the input path
                let target = path.parent().unwrap().join(target);

                // Normalize the final path
                let target = path_clean::clean(target.as_str()).into();
                Ok(target)
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "File was not a symlink",
            ))
        }
    }

    async fn read_to_string(&self, path: impl PathType) -> Result<String> {
        let data = self.read(path).await?;
        let s = String::from_utf8(data).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "File did not contain valid UTF-8",
            )
        })?;
        Ok(s)
    }

    async fn symlink_metadata(&self, path: impl PathType) -> Result<Metadata> {
        // We don't want to follow symlink chains here
        self.open_no_follow_symlink(path).await?.metadata().await
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

#[cfg(test)]
mod tests {
    use lunchbox::{types::PathType, ReadableFileSystem};
    use tokio::io::AsyncReadExt;

    use crate::ZipFS;

    fn get_test_data_dir() -> std::path::PathBuf {
        let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("test_data");
        d
    }

    #[tokio::test]
    async fn test_external_symlink() {
        let zip = ZipFS::new(get_test_data_dir().join("external_symlink.zip")).await;

        assert!(zip.read_link("/symlink_to_etc_passwd").await.is_err());
        assert!(zip.open("/symlink_to_etc_passwd").await.is_err())
    }

    async fn read_file(fs: &ZipFS<std::path::PathBuf>, path: impl PathType) -> String {
        let mut str = String::new();
        fs.open(path)
            .await
            .unwrap()
            .read_to_string(&mut str)
            .await
            .unwrap();
        str
    }

    #[tokio::test]
    async fn test_symlinks() {
        let zip = ZipFS::new(get_test_data_dir().join("symlink_test.zip")).await;

        assert!(zip.metadata("a/a.txt").await.unwrap().is_file());
        assert!(zip.symlink_metadata("a/a.txt").await.unwrap().is_file());
        assert_eq!(zip.read_link("a/b.txt").await.unwrap(), "a/a.txt");
        assert_eq!(zip.read_link("a/c.txt").await.unwrap(), "a/a.txt");
        assert_eq!(zip.read_link("a/d.txt").await.unwrap(), "a/a.txt");
        assert_eq!(
            zip.read_link("b/c/symlink_to_a.txt").await.unwrap(),
            "a/a.txt"
        );

        assert!(zip.metadata("b/c/d.txt").await.unwrap().is_file());
        assert!(zip.symlink_metadata("b/c/d.txt").await.unwrap().is_file());
        assert_eq!(
            zip.read_link("symlink_to_d.txt").await.unwrap(),
            "b/c/d.txt"
        );
        assert_eq!(
            zip.read_link("/symlink_to_d.txt").await.unwrap(),
            "b/c/d.txt"
        );
        assert_eq!(
            zip.read_link("b/symlink_to_d.txt").await.unwrap(),
            "b/c/d.txt"
        );
        assert_eq!(
            zip.read_link("b/./symlink_to_d.txt").await.unwrap(),
            "b/c/d.txt"
        );

        assert_eq!(read_file(&zip, "a/a.txt").await, "a");
        assert_eq!(read_file(&zip, "a/b.txt").await, "a");
        assert_eq!(read_file(&zip, "a/c.txt").await, "a");
        assert_eq!(read_file(&zip, "a/d.txt").await, "a");
        assert_eq!(read_file(&zip, "b/c/symlink_to_a.txt").await, "a");

        assert_eq!(read_file(&zip, "b/c/d.txt").await, "d");
        assert_eq!(read_file(&zip, "b/symlink_to_d.txt").await, "d");
        assert_eq!(read_file(&zip, "symlink_to_d.txt").await, "d");
        assert_eq!(read_file(&zip, "/symlink_to_d.txt").await, "d");
    }
}
