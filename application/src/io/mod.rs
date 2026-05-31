use std::{
    io::{Read, Write},
    ops::{Bound, RangeBounds},
};
use tokio::io::AsyncWriteExt;

pub mod abort;
pub mod compression;
pub mod counting_reader;
pub mod counting_writer;
pub mod fixed_reader;
pub mod hash_reader;
pub mod limited_reader;
pub mod limited_writer;
pub mod range_reader;
pub mod tail;

pub fn copy(
    reader: &mut (impl ?Sized + Read),
    writer: &mut (impl ?Sized + Write),
) -> std::io::Result<()> {
    let mut buffer = vec![0; crate::BUFFER_SIZE];

    copy_shared(&mut buffer, reader, writer)
}

pub fn copy_shared(
    buffer: &mut [u8],
    reader: &mut (impl ?Sized + Read),
    mut writer: &mut (impl ?Sized + Write),
) -> std::io::Result<()> {
    loop {
        let bytes_read = reader.read(buffer)?;

        if crate::unlikely(bytes_read == 0) {
            break;
        }

        writer.safe_write_all(buffer, bytes_read)?;
    }

    Ok(())
}

#[cfg(unix)]
pub fn copy_file_progress(
    reader: &mut (impl std::os::fd::AsFd + Read + ?Sized),
    mut writer: &mut (impl std::os::fd::AsFd + Write + ?Sized),
    mut progress: impl FnMut(usize) -> Result<(), std::io::Error>,
    listener: abort::AbortListener,
) -> Result<u64, std::io::Error> {
    let mut total_copied = 0;

    loop {
        if crate::unlikely(listener.is_aborted()) {
            return Err(std::io::Error::other("Operation aborted"));
        }

        #[cfg(target_os = "linux")]
        let result = rustix::fs::copy_file_range(
            reader.as_fd(),
            None,
            writer.as_fd(),
            None,
            crate::BUFFER_SIZE,
        );
        #[cfg(not(target_os = "linux"))]
        let result = Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "copy_file_range is not supported on this platform",
        ));

        match result {
            Ok(0) => break,
            Ok(bytes_copied) => {
                total_copied += bytes_copied as u64;
                progress(bytes_copied)?;
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::Interrupted => continue,
                std::io::ErrorKind::CrossesDevices | std::io::ErrorKind::Unsupported => {
                    let mut buffer = vec![0; crate::BUFFER_SIZE];

                    loop {
                        if crate::unlikely(listener.is_aborted()) {
                            return Err(std::io::Error::other("Operation aborted"));
                        }

                        let bytes_read = reader.read(&mut buffer)?;
                        if crate::unlikely(bytes_read == 0) {
                            break;
                        }

                        writer.safe_write_all(&buffer, bytes_read)?;

                        total_copied += bytes_read as u64;
                        progress(bytes_read)?;
                    }

                    break;
                }
                _ => return Err(err.into()),
            },
        }
    }

    Ok(total_copied)
}

#[cfg(not(unix))]
pub fn copy_file_progress(
    reader: &mut (impl Read + ?Sized),
    mut writer: &mut (impl Write + ?Sized),
    mut progress: impl FnMut(usize) -> Result<(), std::io::Error>,
    listener: abort::AbortListener,
) -> Result<u64, std::io::Error> {
    let mut total_copied = 0;
    let mut buffer = vec![0; crate::BUFFER_SIZE];

    loop {
        if crate::unlikely(listener.is_aborted()) {
            return Err(std::io::Error::other("Operation aborted"));
        }

        let bytes_read = reader.read(&mut buffer)?;
        if crate::unlikely(bytes_read == 0) {
            break;
        }

        writer.safe_write_all(&buffer, bytes_read)?;

        total_copied += bytes_read as u64;
        progress(bytes_read)?;
    }

    Ok(total_copied)
}

pub trait WriteSeek: Write + std::io::Seek {}
impl<T: Write + std::io::Seek> WriteSeek for T {}
pub trait AsyncWriteSeek: tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin {}
impl<T: tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin> AsyncWriteSeek for T {}
pub trait ReadSeek: Read + std::io::Seek {}
impl<T: Read + std::io::Seek> ReadSeek for T {}
pub trait ReadWriteSeek: Read + Write + std::io::Seek {}
impl<T: Read + Write + std::io::Seek> ReadWriteSeek for T {}
pub trait AsyncReadWriteSeek:
    tokio::io::AsyncRead + tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin
{
}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin>
    AsyncReadWriteSeek for T
{
}

pub trait SafeWrite: Write {
    fn safe_write_all(&mut self, buf: &[u8], start_bytes: usize) -> std::io::Result<()> {
        if crate::unlikely(start_bytes > buf.len()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "start_bytes exceeds buffer length",
            ));
        }

        // SAFETY: Check ensures start_bytes is within buffer bounds
        unsafe { self.write_all(buf.get_unchecked(..start_bytes)) }
    }
}
impl<T: Write> SafeWrite for T {}
pub trait SafeAsyncWrite: tokio::io::AsyncWrite + Unpin {
    async fn safe_write_all(&mut self, buf: &[u8], start_bytes: usize) -> std::io::Result<()> {
        if crate::unlikely(start_bytes > buf.len()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "start_bytes exceeds buffer length",
            ));
        }

        // SAFETY: Check ensures start_bytes is within buffer bounds
        unsafe { self.write_all(buf.get_unchecked(..start_bytes)).await }
    }
}
impl<T: tokio::io::AsyncWrite + Unpin> SafeAsyncWrite for T {}

pub trait SafeDigest: sha2::Digest {
    fn safe_update(&mut self, buf: &[u8], start_bytes: usize) -> std::io::Result<()> {
        if crate::unlikely(start_bytes > buf.len()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "start_bytes exceeds buffer length",
            ));
        }

        // SAFETY: Check ensures start_bytes is within buffer bounds
        unsafe { self.update(buf.get_unchecked(..start_bytes)) };

        Ok(())
    }
}
impl<T: sha2::Digest> SafeDigest for T {}

fn resolve_range(range: impl RangeBounds<usize>, len: usize) -> std::io::Result<(usize, usize)> {
    let start = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n.checked_add(1).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "range start overflows usize",
            )
        })?,
        Bound::Unbounded => 0,
    };

    let end = match range.end_bound() {
        Bound::Included(&n) => n.checked_add(1).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "range end overflows usize",
            )
        })?,
        Bound::Excluded(&n) => n,
        Bound::Unbounded => len,
    };

    if crate::unlikely(start > end) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "range start exceeds range end",
        ));
    }

    if crate::unlikely(end > len) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "range end exceeds slice length",
        ));
    }

    Ok((start, end))
}

pub trait SafeSlice<T>: AsRef<[T]> {
    fn get_slice(&self, range: impl RangeBounds<usize>) -> std::io::Result<&[T]> {
        let slice = self.as_ref();
        let (start, end) = resolve_range(range, slice.len())?;

        // SAFETY: resolve_range guarantees start <= end <= slice.len()
        Ok(unsafe { slice.get_unchecked(start..end) })
    }
}
impl<T, Tr: AsRef<[T]>> SafeSlice<T> for Tr {}

pub trait SafeSliceMut<T>: AsMut<[T]> {
    fn get_slice_mut(&mut self, range: impl RangeBounds<usize>) -> std::io::Result<&mut [T]> {
        let slice = self.as_mut();
        let (start, end) = resolve_range(range, slice.len())?;

        // SAFETY: resolve_range guarantees start <= end <= slice.len()
        Ok(unsafe { slice.get_unchecked_mut(start..end) })
    }
}
impl<T, Tr: AsMut<[T]>> SafeSliceMut<T> for Tr {}
