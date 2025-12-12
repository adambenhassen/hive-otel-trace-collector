use hmac::{Hmac, Mac};
use sha1::Sha1;
use subtle::ConstantTimeEq;

type HmacSha1 = Hmac<Sha1>;

/// Verifies Vercel webhook signatures using HMAC-SHA1.
///
/// Vercel signs webhook payloads with the shared secret and includes
/// the signature in the `X-Vercel-Signature` header as `sha1=<hex_digest>`.
pub struct VercelSignatureVerifier {
    secret: Vec<u8>,
}

impl VercelSignatureVerifier {
    pub fn new(secret: String) -> Self {
        Self {
            secret: secret.into_bytes(),
        }
    }

    /// Verify the X-Vercel-Signature header against the request body.
    ///
    /// The signature format is: `sha1=<hex_digest>`
    ///
    /// Uses constant-time comparison to prevent timing attacks.
    pub fn verify(&self, signature_header: &str, body: &[u8]) -> bool {
        let expected_hex = match signature_header.strip_prefix("sha1=") {
            Some(hex) => hex,
            None => return false,
        };

        let mut mac = match HmacSha1::new_from_slice(&self.secret) {
            Ok(mac) => mac,
            Err(_) => return false,
        };
        mac.update(body);

        let result = mac.finalize();
        let computed_hex = hex::encode(result.into_bytes());

        // Constant-time comparison to prevent timing attacks
        computed_hex.as_bytes().ct_eq(expected_hex.as_bytes()).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_signature() {
        let secret = "test-secret".to_string();
        let verifier = VercelSignatureVerifier::new(secret.clone());

        let body = b"test payload";

        // Compute expected signature
        let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let expected = format!("sha1={}", hex::encode(mac.finalize().into_bytes()));

        assert!(verifier.verify(&expected, body));
    }

    #[test]
    fn test_invalid_signature() {
        let verifier = VercelSignatureVerifier::new("test-secret".to_string());
        let body = b"test payload";

        assert!(!verifier.verify("sha1=invalid", body));
    }

    #[test]
    fn test_missing_prefix() {
        let verifier = VercelSignatureVerifier::new("test-secret".to_string());
        let body = b"test payload";

        assert!(!verifier.verify("invalid-signature", body));
    }

    #[test]
    fn test_wrong_secret() {
        let verifier = VercelSignatureVerifier::new("correct-secret".to_string());
        let body = b"test payload";

        // Compute signature with wrong secret
        let mut mac = HmacSha1::new_from_slice(b"wrong-secret").unwrap();
        mac.update(body);
        let wrong_sig = format!("sha1={}", hex::encode(mac.finalize().into_bytes()));

        assert!(!verifier.verify(&wrong_sig, body));
    }

    #[test]
    fn test_empty_body() {
        let secret = "test-secret".to_string();
        let verifier = VercelSignatureVerifier::new(secret.clone());

        let body = b"";

        let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let expected = format!("sha1={}", hex::encode(mac.finalize().into_bytes()));

        assert!(verifier.verify(&expected, body));
    }
}
