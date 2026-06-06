use crate::io::SafeSliceMutExt;
use futures::ready;
use std::{
    io::{self, Read, Seek, SeekFrom},
    ops::Bound,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt, ReadBuf};

fn resolve_range(range: (Bound<u64>, Bound<u64>), len: u64) -> io::Result<(u64, u64)> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "invalid range specified");

    let last = len.checked_sub(1).ok_or_else(invalid)?;

    let start = match range.0 {
        Bound::Included(start) => start,
        Bound::Excluded(start) => start.checked_add(1).ok_or_else(invalid)?,
        Bound::Unbounded => 0,
    };

    let end = match range.1 {
        Bound::Included(end) => end.min(last),
        Bound::Excluded(end) => end.checked_sub(1).ok_or_else(invalid)?.min(last),
        Bound::Unbounded => last,
    };

    if start > end {
        return Err(invalid());
    }

    Ok((start, end))
}

pub struct RangeReader<R> {
    inner: R,
    start: u64,
    end: u64,
    pos: u64,
}

impl<R: Read + Seek> RangeReader<R> {
    pub fn new(
        mut inner: R,
        range: impl Into<(Bound<u64>, Bound<u64>)>,
        len: u64,
    ) -> io::Result<Self> {
        let (start, end) = resolve_range(range.into(), len)?;

        inner.seek(SeekFrom::Start(start))?;

        Ok(Self {
            inner,
            start,
            end,
            pos: start,
        })
    }

    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }
}

impl<R: Read + Seek> Read for RangeReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if crate::unlikely(self.pos > self.end) {
            return Ok(0);
        }

        let remaining = self.end - self.pos + 1;
        let to_read = remaining.min(buf.len() as u64) as usize;

        let bytes_read = self.inner.read(buf.get_slice_mut(..to_read)?)?;
        self.pos += bytes_read as u64;

        Ok(bytes_read)
    }
}

pub struct AsyncRangeReader<R> {
    inner: R,
    start: u64,
    end: u64,
    pos: u64,
}

impl<R: AsyncRead + AsyncSeek + Unpin> AsyncRangeReader<R> {
    pub async fn new(
        mut inner: R,
        range: impl Into<(Bound<u64>, Bound<u64>)>,
        len: u64,
    ) -> io::Result<Self> {
        let (start, end) = resolve_range(range.into(), len)?;

        inner.seek(SeekFrom::Start(start)).await?;

        Ok(Self {
            inner,
            start,
            end,
            pos: start,
        })
    }

    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> AsyncRead for AsyncRangeReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = &mut *self;

        if crate::unlikely(me.pos > me.end) {
            return Poll::Ready(Ok(()));
        }

        let remaining = me.end - me.pos + 1;
        let to_read = remaining.min(buf.remaining() as u64) as usize;

        if crate::unlikely(to_read == 0) {
            return Poll::Ready(Ok(()));
        }

        let mut tmp = ReadBuf::new(buf.initialize_unfilled_to(to_read));

        ready!(Pin::new(&mut me.inner).poll_read(cx, &mut tmp))?;

        let bytes_read = tmp.filled().len();

        buf.advance(bytes_read);
        me.pos += bytes_read as u64;

        Poll::Ready(Ok(()))
    }
}
