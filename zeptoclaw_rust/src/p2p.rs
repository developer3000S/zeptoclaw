//! Порт пакета `internal/p2p/framing.go`: транспорт TCP с length-prefixed
//! protobuf-кадрами. Кадр = uvarint-длина + полезная нагрузка protobuf.
//!
//! Go-реализация использует libp2p; в Rust-порте транспорт — обычный TCP с
//! теми же фреймами, что сохраняет совместимость уровня сообщений.

use anyhow::{bail, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::proto::{Message, RpcRequest, RpcResponse, TaskEnvelope, TaskResult};

/// Жёсткий потолок одного кадра — защита от пира, заявившего абсурдную длину.
pub const MAX_FRAME_BYTES: usize = 8 << 20;

/// Записывает кадр: uvarint-префикс длины + полезная нагрузка.
pub async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, payload: &[u8]) -> Result<()> {
    let mut hdr = [0u8; 10]; // binary.MaxVarintLen64
    let n = encode_uvarint(payload.len() as u64, &mut hdr);
    w.write_all(&hdr[..n]).await?;
    w.write_all(payload).await?;
    w.flush().await?;
    Ok(())
}

/// Читает один length-prefixed кадр с потолками `limit` (0 — без лимита).
pub async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R, limit: usize) -> Result<Vec<u8>> {
    let size = read_uvarint(r).await?;
    if size == 0 {
        return Ok(Vec::new());
    }
    let size = size as usize;
    if size > MAX_FRAME_BYTES {
        bail!("p2p: frame of {size} bytes exceeds protocol cap");
    }
    if limit > 0 && size > limit {
        bail!("p2p: frame of {size} bytes exceeds configured limit {limit}");
    }
    let mut buf = vec![0u8; size];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Маршалирует protobuf-сообщение и пишет его кадром.
pub async fn write_msg<W: AsyncWriteExt + Unpin, M: Message>(w: &mut W, m: &M) -> Result<()> {
    write_frame(w, &m.encode()).await
}

/// Читает кадр и разбирает его как protobuf-сообщение.
pub async fn read_msg<R: AsyncReadExt + Unpin, M: Message>(
    r: &mut R,
    limit: usize,
) -> Result<M> {
    let b = read_frame(r, limit).await?;
    if b.is_empty() {
        bail!("p2p: empty message frame");
    }
    M::decode(&b).map_err(|e| anyhow::anyhow!("p2p: unmarshal: {e:?}"))
}

// ---- Типизированные помощники для потоков mesh (как в internal/p2p/host.go) ----

pub async fn write_envelope<W: AsyncWriteExt + Unpin>(w: &mut W, m: &TaskEnvelope) -> Result<()> {
    write_msg(w, m).await
}

pub async fn read_envelope<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<TaskEnvelope> {
    read_msg(r, 0).await
}

pub async fn write_result<W: AsyncWriteExt + Unpin>(w: &mut W, m: &TaskResult) -> Result<()> {
    write_msg(w, m).await
}

pub async fn read_result<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<TaskResult> {
    read_msg(r, 0).await
}

pub async fn write_rpc_request<W: AsyncWriteExt + Unpin>(w: &mut W, m: &RpcRequest) -> Result<()> {
    write_msg(w, m).await
}

pub async fn read_rpc_request<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<RpcRequest> {
    read_msg(r, 0).await
}

pub async fn write_rpc_response<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    m: &RpcResponse,
) -> Result<()> {
    write_msg(w, m).await
}

pub async fn read_rpc_response<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<RpcResponse> {
    read_msg(r, 0).await
}

/// Кодирует uvarint в буфер, возвращает число записанных байт.
fn encode_uvarint(mut v: u64, out: &mut [u8; 10]) -> usize {
    let mut i = 0;
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out[i] = byte;
        i += 1;
        if v == 0 {
            break;
        }
    }
    i
}

/// Читает uvarint из потока (побайтово, как Go's byteReader).
async fn read_uvarint<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let mut b = [0u8; 1];
        r.read_exact(&mut b).await?;
        let byte = b[0];
        result |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            bail!("p2p: uvarint overflow in frame header");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn uvarint_matches_go_binary_putuvarint() {
        let cases: [(u64, &[u8]); 5] = [
            (0, &[0x00]),
            (1, &[0x01]),
            (127, &[0x7f]),
            (128, &[0x80, 0x01]),
            (300, &[0xac, 0x02]),
        ];
        for (v, want) in cases {
            let mut buf = [0u8; 10];
            let n = encode_uvarint(v, &mut buf);
            assert_eq!(&buf[..n], want, "uvarint({v})");
        }
    }

    #[tokio::test]
    async fn frame_roundtrip() {
        let (mut a, mut b) = duplex(4096);
        let payload = b"hello mesh".to_vec();
        let p2 = payload.clone();
        let h = tokio::spawn(async move {
            write_frame(&mut a, &p2).await.unwrap();
        });
        let got = read_frame(&mut b, 0).await.unwrap();
        h.await.unwrap();
        assert_eq!(got, payload);
    }

    #[tokio::test]
    async fn frame_rejects_oversize_announcement() {
        let (mut a, mut b) = duplex(64);
        // Заявим длину больше MAX_FRAME_BYTES.
        let mut hdr = [0u8; 10];
        let n = encode_uvarint((MAX_FRAME_BYTES + 1) as u64, &mut hdr);
        a.write_all(&hdr[..n]).await.unwrap();
        let err = read_frame(&mut b, 0).await.unwrap_err();
        assert!(err.to_string().contains("exceeds protocol cap"));
    }

    #[tokio::test]
    async fn empty_frame_reads_as_empty() {
        let (mut a, mut b) = duplex(64);
        a.write_all(&[0x00]).await.unwrap();
        let got = read_frame(&mut b, 0).await.unwrap();
        assert!(got.is_empty());
    }
}
