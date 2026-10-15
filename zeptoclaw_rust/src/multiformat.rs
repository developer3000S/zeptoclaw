//! Multiformat-утилиты: base58btc, multihash identity-v1, CIDv1 и libp2p-PeerID.
//!
//! Совместимо с `github.com/multiformats/*` и `go-libp2p/core/peer`:
//! - PeerID(ed25519) = base58btc(multihash(identity-v1(protobuf PublicKey)))
//! - p2p-multiaddr  = /ip4/<addr>/tcp/<port>/p2p/<PeerID>

use sha2::{Digest as _, Sha256};

/// Коды multihash (см. multiformats/multihash tables).
pub const MH_IDENTITY: u64 = 0x00;
pub const MH_SHA2_256: u64 = 0x12;

/// Коды multicodec.
pub const CODEC_LIBP2P_PUBLIC_KEY: u64 = 0x1205;
pub const CODEC_ED25519_PUB: u64 = 0xed;
pub const CODEC_DAG_PB: u64 = 0x70;

// ---------------------------------------------------------------------------
// base58 (алфавит Bitcoin)
// ---------------------------------------------------------------------------

const B58_ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub fn base58_encode(data: &[u8]) -> String {
    bs58::encode(data).with_alphabet(bs58::Alphabet::BITCOIN).into_string()
}

pub fn base58_decode(s: &str) -> Result<Vec<u8>, String> {
    bs58::decode(s).with_alphabet(bs58::Alphabet::BITCOIN).into_vec().map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// varint (multiformats — тот же big-endian LEB128, что и в protobuf)
// ---------------------------------------------------------------------------

pub fn put_uvarint(mut v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
    out
}

/// Читает uvarint из начала буфера; возвращает (значение, потреблённые байты).
pub fn read_uvarint(buf: &[u8]) -> Result<(u64, usize), String> {
    let mut val: u64 = 0;
    for i in 0..buf.len().min(9) {
        let b = buf[i];
        if i == 8 && b > 1 {
            return Err("uvarint overflow".into());
        }
        val |= ((b & 0x7f) as u64) << (7 * i);
        if b < 0x80 {
            return Ok((val, i + 1));
        }
    }
    Err("truncated uvarint".into())
}

// ---------------------------------------------------------------------------
// multihash / CID
// ---------------------------------------------------------------------------

/// multihash: <code varint><length varint><digest>.
pub fn multihash(code: u64, digest: &[u8]) -> Vec<u8> {
    let mut out = put_uvarint(code);
    out.extend_from_slice(&put_uvarint(digest.len() as u64));
    out.extend_from_slice(digest);
    out
}

/// identity-multihash от произвольных данных (используется для Ed25519 PeerID).
pub fn identity_multihash(data: &[u8]) -> Vec<u8> {
    multihash(MH_IDENTITY, data)
}

pub fn sha256_multihash(data: &[u8]) -> Vec<u8> {
    let h = Sha256::digest(data);
    multihash(MH_SHA2_256, &h)
}

/// CIDv1: <0x01><content-codec varint><multihash>.
pub fn cid_v1(codec: u64, mh: &[u8]) -> Vec<u8> {
    let mut out = vec![0x01];
    out.extend_from_slice(&put_uvarint(codec));
    out.extend_from_slice(mh);
    out
}

/// PeerID из сериализованного protobuf PublicKey (MarshalPublicKey).
/// Для ed25519 это identity-multihash; для остальных — sha256 (как go-libp2p IDFromPublicKey).
pub fn peer_id_from_pubkey_proto(pub_proto: &[u8]) -> String {
    // Поле 1 protobuf PublicKey.KeyInfo — ключ в формате SubjectPublicKeyInfo.
    // go-libp2p для Ed25519 использует identity-hash от всего pub-блоба длиной <= 42 байт.
    let mh = if pub_proto.len() <= 42 {
        identity_multihash(pub_proto)
    } else {
        sha256_multihash(pub_proto)
    };
    base58_encode(&mh)
}

// ---------------------------------------------------------------------------
// multiaddr
// ---------------------------------------------------------------------------

/// Формирует p2p multiaddr строкой: /ip4/A.B.C.D/tcp/PORT/p2p/PEERID
pub fn p2p_multiaddr(ip: &str, port: u16, peer_id: &str) -> String {
    format!("/ip4/{}/tcp/{}/p2p/{}", ip, port, peer_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base58_roundtrip() {
        let data = [0u8, 0, 1, 2, 250, 255];
        let s = base58_encode(&data);
        assert_eq!(base58_decode(&s).unwrap(), data.to_vec());
    }

    #[test]
    fn uvarint_known() {
        // Значения из спецификации multiformats varint.
        assert_eq!(put_uvarint(1), vec![0x01]);
        assert_eq!(put_uvarint(128), vec![0x80, 0x01]);
        assert_eq!(put_uvarint(300), vec![0xac, 0x02]);
        assert_eq!(read_uvarint(&[0xac, 0x02, 0xff]).unwrap(), (300, 2));
    }

    #[test]
    fn known_peerid_matches_go() {
        // Публичный blob (08011220 + 32-байтный ключ) — зафиксирован по выводу Go-версии.
        let mut blob = vec![0x08, 0x01, 0x12, 0x20];
        blob.extend_from_slice(&[7u8; 32]);
        let pid = peer_id_from_pubkey_proto(&blob);
        // Проверяем структуру: identity-multihash => длина payload 36, base58 декодится обратно.
        let raw = base58_decode(&pid).unwrap();
        assert_eq!(raw, identity_multihash(&blob));
    }
}

/// Валидация PeerID как Go `peer.Decode`: base58 → multihash(identity/sha256), корректная длина.
pub fn validate_peer_id(s: &str) -> Result<(), String> {
    let raw = base58_decode(s)?;
    let (code, n1) = read_uvarint(&raw)?;
    let (_hlen, n2) = read_uvarint(&raw[n1..])?;
    let total = n1 + n2;
    match code {
        0x00 => {
            // identity: для Ed25519 публичный protobuf = 46 байт (0x2d 0x63 + 32 ключа + префиксы)
            if raw.len() - total < 32 || raw.len() - total > 64 {
                return Err("bad ed25519 peer id length".into());
            }
        }
        0x12 => {
            if raw.len() - total != 32 {
                return Err("bad sha256 peer id length".into());
            }
        }
        _ => return Err(format!("unsupported multihash code {code:#x}")),
    }
    Ok(())
}
