// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::OnceLock, time::Duration};

use blst::{
    min_sig::{PublicKey as BlsPublicKey, SecretKey as BlsSecretKey, Signature as BlsSignature},
    BLST_ERROR,
};
use ed25519_dalek::{
    Signature as Ed25519Signature, Signer, SigningKey as Ed25519SigningKey, Verifier,
    VerifyingKey as Ed25519VerifyingKey,
};
use k256::ecdsa::{
    signature::hazmat::{
        PrehashSigner as K256PrehashSigner, PrehashVerifier as K256PrehashVerifier,
    },
    Signature as Secp256k1EcdsaSignature, SigningKey as Secp256k1EcdsaSigningKey,
    VerifyingKey as Secp256k1EcdsaVerifyingKey,
};
use k256::schnorr::{
    Signature as Secp256k1SchnorrSignature, SigningKey as Secp256k1SchnorrSigningKey,
    VerifyingKey as Secp256k1SchnorrVerifyingKey,
};
use p256::ecdsa::{
    signature::hazmat::{
        PrehashSigner as P256PrehashSigner, PrehashVerifier as P256PrehashVerifier,
    },
    Signature as P256EcdsaSignature, SigningKey as P256EcdsaSigningKey,
    VerifyingKey as P256EcdsaVerifyingKey,
};
use sui_types::digests::TransactionDigest;

use crate::{
    config::StatelessVerificationMode,
    executor::api::{StatelessCryptoScheme, StatelessVerificationRequest},
};

const ED25519_SECRET_KEY_BYTES: [u8; 32] = [7; 32];
const BLS_IKM: [u8; 32] = [11; 32];
const SECP256K1_SECRET_KEY_BYTES: [u8; 32] = [13; 32];
const P256_SECRET_KEY_BYTES: [u8; 32] = [17; 32];
const SCHNORR_SECRET_KEY_BYTES: [u8; 32] = [19; 32];
const SCHNORR_AUX_RANDOM_BYTES: [u8; 32] = [23; 32];
const BLS_MIN_SIG_DST: &[u8] = b"BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_NUL_";

struct BlsMaterial {
    secret_key_bytes: [u8; 32],
    public_key_bytes: [u8; 96],
}

fn ed25519_public_key_bytes() -> &'static [u8; 32] {
    static PUBLIC_KEY_BYTES: OnceLock<[u8; 32]> = OnceLock::new();
    PUBLIC_KEY_BYTES.get_or_init(|| {
        let signing_key = Ed25519SigningKey::from_bytes(&ED25519_SECRET_KEY_BYTES);
        signing_key.verifying_key().to_bytes()
    })
}

fn secp256k1_public_key_bytes() -> &'static [u8; 33] {
    static PUBLIC_KEY_BYTES: OnceLock<[u8; 33]> = OnceLock::new();
    PUBLIC_KEY_BYTES.get_or_init(|| {
        let signing_key = Secp256k1EcdsaSigningKey::from_bytes(&SECP256K1_SECRET_KEY_BYTES)
            .expect("secp256k1 secret key bytes should deserialize");
        signing_key.verifying_key().to_bytes().into()
    })
}

fn p256_public_key_bytes() -> &'static [u8; 33] {
    static PUBLIC_KEY_BYTES: OnceLock<[u8; 33]> = OnceLock::new();
    PUBLIC_KEY_BYTES.get_or_init(|| {
        let signing_key = P256EcdsaSigningKey::from_slice(&P256_SECRET_KEY_BYTES)
            .expect("P-256 secret key bytes should deserialize");
        signing_key
            .verifying_key()
            .to_encoded_point(true)
            .as_bytes()
            .try_into()
            .expect("Compressed P-256 public key should be 33 bytes")
    })
}

fn schnorr_public_key_bytes() -> &'static [u8; 32] {
    static PUBLIC_KEY_BYTES: OnceLock<[u8; 32]> = OnceLock::new();
    PUBLIC_KEY_BYTES.get_or_init(|| {
        let signing_key = Secp256k1SchnorrSigningKey::from_bytes(&SCHNORR_SECRET_KEY_BYTES)
            .expect("Schnorr secret key bytes should deserialize");
        signing_key.verifying_key().to_bytes().into()
    })
}

fn bls_material() -> &'static BlsMaterial {
    static MATERIAL: OnceLock<BlsMaterial> = OnceLock::new();
    MATERIAL.get_or_init(|| {
        let secret_key =
            BlsSecretKey::key_gen(&BLS_IKM, &[]).expect("BLS IKM should derive a secret key");
        let public_key = secret_key.sk_to_pk();
        BlsMaterial {
            secret_key_bytes: secret_key.serialize(),
            public_key_bytes: public_key.to_bytes(),
        }
    })
}

fn digest_array(digest: TransactionDigest) -> [u8; 32] {
    digest.into_inner()
}

pub fn make_verification_request(
    mode: StatelessVerificationMode,
    digest: TransactionDigest,
    duration: Duration,
) -> StatelessVerificationRequest {
    match mode {
        StatelessVerificationMode::Synthetic => {
            StatelessVerificationRequest::synthetic(digest, duration)
        }
        StatelessVerificationMode::Ed25519 => make_ed25519_request(digest, duration),
        StatelessVerificationMode::Bls => make_bls_request(digest, duration),
        StatelessVerificationMode::Secp256k1 => make_secp256k1_request(digest, duration),
        StatelessVerificationMode::P256 => make_p256_request(digest, duration),
        StatelessVerificationMode::Schnorr => make_schnorr_request(digest, duration),
    }
}

pub fn verify_request(request: &StatelessVerificationRequest) -> bool {
    let StatelessVerificationRequest::Crypto {
        digest,
        scheme,
        public_key,
        signature,
        ..
    } = request
    else {
        return false;
    };

    match scheme {
        StatelessCryptoScheme::Ed25519Dalek => {
            verify_ed25519_request(*digest, public_key, signature)
        }
        StatelessCryptoScheme::Bls12381MinSig => verify_bls_request(*digest, public_key, signature),
        StatelessCryptoScheme::Secp256k1Ecdsa => {
            verify_secp256k1_request(*digest, public_key, signature)
        }
        StatelessCryptoScheme::P256Ecdsa => verify_p256_request(*digest, public_key, signature),
        StatelessCryptoScheme::Secp256k1Schnorr => {
            verify_schnorr_request(*digest, public_key, signature)
        }
    }
}

fn make_ed25519_request(
    digest: TransactionDigest,
    duration: Duration,
) -> StatelessVerificationRequest {
    let signing_key = Ed25519SigningKey::from_bytes(&ED25519_SECRET_KEY_BYTES);
    let signature = signing_key.sign(digest.as_ref());

    StatelessVerificationRequest::crypto(
        digest,
        duration,
        StatelessCryptoScheme::Ed25519Dalek,
        ed25519_public_key_bytes().to_vec(),
        signature.to_bytes().to_vec(),
    )
}

fn make_secp256k1_request(
    digest: TransactionDigest,
    duration: Duration,
) -> StatelessVerificationRequest {
    let signing_key = Secp256k1EcdsaSigningKey::from_bytes(&SECP256K1_SECRET_KEY_BYTES)
        .expect("secp256k1 secret key bytes should deserialize");
    let signature: Secp256k1EcdsaSignature =
        K256PrehashSigner::sign_prehash(&signing_key, digest.inner())
            .expect("32-byte digest should be signable with secp256k1");

    StatelessVerificationRequest::crypto(
        digest,
        duration,
        StatelessCryptoScheme::Secp256k1Ecdsa,
        secp256k1_public_key_bytes().to_vec(),
        signature.as_ref().to_vec(),
    )
}

fn make_p256_request(
    digest: TransactionDigest,
    duration: Duration,
) -> StatelessVerificationRequest {
    let signing_key = P256EcdsaSigningKey::from_slice(&P256_SECRET_KEY_BYTES)
        .expect("P-256 secret key bytes should deserialize");
    let signature: P256EcdsaSignature =
        P256PrehashSigner::sign_prehash(&signing_key, digest.inner())
            .expect("32-byte digest should be signable with P-256");

    StatelessVerificationRequest::crypto(
        digest,
        duration,
        StatelessCryptoScheme::P256Ecdsa,
        p256_public_key_bytes().to_vec(),
        signature.to_vec(),
    )
}

fn make_schnorr_request(
    digest: TransactionDigest,
    duration: Duration,
) -> StatelessVerificationRequest {
    let signing_key = Secp256k1SchnorrSigningKey::from_bytes(&SCHNORR_SECRET_KEY_BYTES)
        .expect("Schnorr secret key bytes should deserialize");
    let digest_bytes = digest_array(digest);
    let signature = signing_key
        .try_sign_prehashed(&digest_bytes, &SCHNORR_AUX_RANDOM_BYTES)
        .expect("32-byte digest should be signable with Schnorr");

    StatelessVerificationRequest::crypto(
        digest,
        duration,
        StatelessCryptoScheme::Secp256k1Schnorr,
        schnorr_public_key_bytes().to_vec(),
        signature.as_ref().to_vec(),
    )
}

fn make_bls_request(digest: TransactionDigest, duration: Duration) -> StatelessVerificationRequest {
    let material = bls_material();
    let secret_key = BlsSecretKey::from_bytes(&material.secret_key_bytes)
        .expect("Cached BLS secret key bytes should deserialize");
    let signature = secret_key.sign(digest.as_ref(), BLS_MIN_SIG_DST, &[]);

    StatelessVerificationRequest::crypto(
        digest,
        duration,
        StatelessCryptoScheme::Bls12381MinSig,
        material.public_key_bytes.to_vec(),
        signature.to_bytes().to_vec(),
    )
}

fn verify_ed25519_request(digest: TransactionDigest, public_key: &[u8], signature: &[u8]) -> bool {
    let public_key: [u8; 32] = match public_key.try_into() {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };
    let signature: [u8; 64] = match signature.try_into() {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };
    let verifying_key = match Ed25519VerifyingKey::from_bytes(&public_key) {
        Ok(key) => key,
        Err(_) => return false,
    };
    let signature = Ed25519Signature::from_bytes(&signature);

    verifying_key.verify(digest.as_ref(), &signature).is_ok()
}

fn verify_secp256k1_request(
    digest: TransactionDigest,
    public_key: &[u8],
    signature: &[u8],
) -> bool {
    let verifying_key = match Secp256k1EcdsaVerifyingKey::from_sec1_bytes(public_key) {
        Ok(key) => key,
        Err(_) => return false,
    };
    let signature = match Secp256k1EcdsaSignature::try_from(signature) {
        Ok(sig) => sig,
        Err(_) => return false,
    };

    K256PrehashVerifier::verify_prehash(&verifying_key, digest.as_ref(), &signature).is_ok()
}

fn verify_p256_request(digest: TransactionDigest, public_key: &[u8], signature: &[u8]) -> bool {
    let verifying_key = match P256EcdsaVerifyingKey::from_sec1_bytes(public_key) {
        Ok(key) => key,
        Err(_) => return false,
    };
    let signature = match P256EcdsaSignature::from_slice(signature) {
        Ok(sig) => sig,
        Err(_) => return false,
    };

    P256PrehashVerifier::verify_prehash(&verifying_key, digest.as_ref(), &signature).is_ok()
}

fn verify_schnorr_request(digest: TransactionDigest, public_key: &[u8], signature: &[u8]) -> bool {
    let verifying_key = match Secp256k1SchnorrVerifyingKey::from_bytes(public_key) {
        Ok(key) => key,
        Err(_) => return false,
    };
    let signature = match Secp256k1SchnorrSignature::try_from(signature) {
        Ok(sig) => sig,
        Err(_) => return false,
    };
    let digest_bytes = digest_array(digest);

    verifying_key
        .verify_prehashed(&digest_bytes, &signature)
        .is_ok()
}

fn verify_bls_request(digest: TransactionDigest, public_key: &[u8], signature: &[u8]) -> bool {
    let public_key = match BlsPublicKey::from_bytes(public_key) {
        Ok(key) => key,
        Err(_) => return false,
    };
    let signature = match BlsSignature::from_bytes(signature) {
        Ok(sig) => sig,
        Err(_) => return false,
    };

    signature.verify(
        true,
        digest.as_ref(),
        BLS_MIN_SIG_DST,
        &[],
        &public_key,
        true,
    ) == BLST_ERROR::BLST_SUCCESS
}

#[cfg(test)]
mod tests {
    use std::{hint::black_box, time::Duration};

    use sui_types::digests::TransactionDigest;

    use super::{make_verification_request, verify_request};
    use crate::config::StatelessVerificationMode;
    use crate::executor::api::StatelessVerificationRequest;

    fn crypto_modes() -> [StatelessVerificationMode; 5] {
        [
            StatelessVerificationMode::Ed25519,
            StatelessVerificationMode::Bls,
            StatelessVerificationMode::Secp256k1,
            StatelessVerificationMode::P256,
            StatelessVerificationMode::Schnorr,
        ]
    }

    #[test]
    fn ed25519_request_verifies() {
        let request = make_verification_request(
            StatelessVerificationMode::Ed25519,
            TransactionDigest::random(),
            Duration::from_micros(10),
        );

        assert!(verify_request(&request));
    }

    #[test]
    fn bls_request_verifies() {
        let request = make_verification_request(
            StatelessVerificationMode::Bls,
            TransactionDigest::random(),
            Duration::from_micros(10),
        );

        assert!(verify_request(&request));
    }

    #[test]
    fn secp256k1_request_verifies() {
        let request = make_verification_request(
            StatelessVerificationMode::Secp256k1,
            TransactionDigest::random(),
            Duration::from_micros(10),
        );

        assert!(verify_request(&request));
    }

    #[test]
    fn p256_request_verifies() {
        let request = make_verification_request(
            StatelessVerificationMode::P256,
            TransactionDigest::random(),
            Duration::from_micros(10),
        );

        assert!(verify_request(&request));
    }

    #[test]
    fn schnorr_request_verifies() {
        let request = make_verification_request(
            StatelessVerificationMode::Schnorr,
            TransactionDigest::random(),
            Duration::from_micros(10),
        );

        assert!(verify_request(&request));
    }

    #[test]
    fn tampered_crypto_request_fails() {
        for mode in crypto_modes() {
            let request = make_verification_request(
                mode,
                TransactionDigest::random(),
                Duration::from_micros(10),
            );
            let StatelessVerificationRequest::Crypto { mut signature, .. } = request.clone() else {
                panic!("expected crypto request");
            };
            signature[0] ^= 0x1;

            let tampered = match request {
                StatelessVerificationRequest::Crypto {
                    digest,
                    cost_hint,
                    scheme,
                    public_key,
                    ..
                } => StatelessVerificationRequest::crypto(
                    digest, cost_hint, scheme, public_key, signature,
                ),
                StatelessVerificationRequest::Synthetic { .. } => unreachable!(),
            };

            assert!(!verify_request(&tampered));
        }
    }

    #[test]
    #[ignore = "stress test; run with --ignored --nocapture"]
    fn stress_stateless_crypto_execution_times() {
        stress_mode("ED25519", StatelessVerificationMode::Ed25519, 2_000);
        stress_mode("BLS", StatelessVerificationMode::Bls, 50);
        stress_mode("SECP256K1", StatelessVerificationMode::Secp256k1, 1_000);
        stress_mode("P256", StatelessVerificationMode::P256, 1_000);
        stress_mode("SCHNORR", StatelessVerificationMode::Schnorr, 1_000);
    }

    fn stress_mode(label: &str, mode: StatelessVerificationMode, iterations: usize) {
        let digests: Vec<_> = (0..iterations)
            .map(|_| TransactionDigest::random())
            .collect();

        let start = std::time::Instant::now();
        let requests: Vec<_> = digests
            .iter()
            .map(|digest| black_box(make_verification_request(mode, *digest, Duration::ZERO)))
            .collect();
        let request_elapsed = start.elapsed();

        let start = std::time::Instant::now();
        let verified = requests
            .iter()
            .filter(|request| black_box(verify_request(black_box(request))))
            .count();
        let verify_elapsed = start.elapsed();

        assert_eq!(verified, iterations);

        let request_avg_us = request_elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
        let verify_avg_us = verify_elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
        let request_ops = iterations as f64 / request_elapsed.as_secs_f64();
        let verify_ops = iterations as f64 / verify_elapsed.as_secs_f64();

        println!(
            "{label}: iterations={iterations}, request_total={:?}, request_avg_us={:.3}, request_ops_per_sec={:.1}, verify_total={:?}, verify_avg_us={:.3}, verify_ops_per_sec={:.1}",
            request_elapsed,
            request_avg_us,
            request_ops,
            verify_elapsed,
            verify_avg_us,
            verify_ops
        );
    }
}
