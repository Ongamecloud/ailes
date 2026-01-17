use std::io::{Read, Write};

pub mod abort;
pub mod compression;
pub mod counting_reader;
pub mod counting_writer;
pub mod fixed_reader;
pub mod hash_reader;
pub mod limited_reader;
pub mod limited_writer;
pub mod range_reader;

pub trait ReadSeek: Read + std::io::Seek {}
impl<T: Read + std::io::Seek> ReadSeek for T {}

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
    writer: &mut (impl ?Sized + Write),
) -> std::io::Result<()> {
    loop {
        let bytes_read = reader.read(buffer)?;

        if crate::unlikely(bytes_read == 0) {
            break;
        }

        writer.write_all(&buffer[..bytes_read])?;
    }

    Ok(())
}

pub trait WriteSeek: Write + std::io::Seek {}
impl<T: Write + std::io::Seek> WriteSeek for T {}
pub trait AsyncWriteSeek: tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin {}
impl<T: tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin> AsyncWriteSeek for T {}
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
