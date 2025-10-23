//! SSL/TLS support for ClickHouse connections
//!
//! This module is only available when the `tls` feature is enabled.

#[cfg(feature = "tls")]
use rustls::{
    Certificate,
    ClientConfig,
    PrivateKey,
    RootCertStore,
};
#[cfg(feature = "tls")]
use std::fs::File;
#[cfg(feature = "tls")]
use std::io::BufReader;
#[cfg(feature = "tls")]
use std::path::PathBuf;
#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
use crate::{
    Error,
    Result,
};

/// SSL/TLS configuration options
#[cfg(feature = "tls")]
#[derive(Clone, Debug)]
pub struct SSLOptions {
    /// Path to CA certificate file(s)
    pub ca_cert_paths: Vec<PathBuf>,
    /// Path to CA certificate directory
    pub ca_cert_directory: Option<PathBuf>,
    /// Use system default CA certificates
    pub use_system_certs: bool,
    /// Path to client certificate (for mutual TLS)
    pub client_cert_path: Option<PathBuf>,
    /// Path to client private key (for mutual TLS)
    pub client_key_path: Option<PathBuf>,
    /// Skip certificate verification (INSECURE - for testing only)
    pub skip_verification: bool,
    /// Enable SNI (Server Name Indication)
    pub use_sni: bool,
    /// Server name for SNI (if different from host)
    pub server_name: Option<String>,
}

#[cfg(feature = "tls")]
impl Default for SSLOptions {
    fn default() -> Self {
        Self {
            ca_cert_paths: Vec::new(),
            ca_cert_directory: None,
            use_system_certs: true,
            client_cert_path: None,
            client_key_path: None,
            skip_verification: false,
            use_sni: true,
            server_name: None,
        }
    }
}

#[cfg(feature = "tls")]
impl SSLOptions {
    /// Create new SSL options
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a CA certificate file
    pub fn add_ca_cert(mut self, path: PathBuf) -> Self {
        self.ca_cert_paths.push(path);
        self
    }

    /// Set CA certificate directory
    pub fn ca_cert_directory(mut self, path: PathBuf) -> Self {
        self.ca_cert_directory = Some(path);
        self
    }

    /// Enable/disable system certificates
    pub fn use_system_certs(mut self, enabled: bool) -> Self {
        self.use_system_certs = enabled;
        self
    }

    /// Set client certificate (for mutual TLS)
    pub fn client_cert(
        mut self,
        cert_path: PathBuf,
        key_path: PathBuf,
    ) -> Self {
        self.client_cert_path = Some(cert_path);
        self.client_key_path = Some(key_path);
        self
    }

    /// Skip certificate verification (INSECURE - for testing only)
    pub fn skip_verification(mut self, skip: bool) -> Self {
        self.skip_verification = skip;
        self
    }

    /// Enable/disable SNI
    pub fn use_sni(mut self, enabled: bool) -> Self {
        self.use_sni = enabled;
        self
    }

    /// Set server name for SNI
    pub fn server_name(mut self, name: String) -> Self {
        self.server_name = Some(name);
        self
    }

    /// Build a rustls ClientConfig from these options
    pub fn build_client_config(&self) -> Result<Arc<ClientConfig>> {
        let mut root_store = RootCertStore::empty();

        // Load system certificates if requested
        if self.use_system_certs {
            let certs =
                rustls_native_certs::load_native_certs().map_err(|e| {
                    Error::Connection(format!(
                        "Failed to load system certs: {}",
                        e
                    ))
                })?;

            for cert in certs {
                root_store.add(&Certificate(cert.0)).map_err(|e| {
                    Error::Connection(format!(
                        "Failed to add system cert: {}",
                        e
                    ))
                })?;
            }
        }

        // Load CA certificates from files
        for ca_path in &self.ca_cert_paths {
            let file = File::open(ca_path).map_err(|e| {
                Error::Connection(format!(
                    "Failed to open CA cert {:?}: {}",
                    ca_path, e
                ))
            })?;
            let mut reader = BufReader::new(file);

            let certs = rustls_pemfile::certs(&mut reader).map_err(|e| {
                Error::Connection(format!(
                    "Failed to parse CA cert {:?}: {}",
                    ca_path, e
                ))
            })?;

            for cert in certs {
                root_store.add(&Certificate(cert)).map_err(|e| {
                    Error::Connection(format!("Failed to add CA cert: {}", e))
                })?;
            }
        }

        // Load CA certificates from directory
        if let Some(ca_dir) = &self.ca_cert_directory {
            let entries = std::fs::read_dir(ca_dir).map_err(|e| {
                Error::Connection(format!(
                    "Failed to read CA cert directory {:?}: {}",
                    ca_dir, e
                ))
            })?;

            for entry in entries {
                let entry = entry.map_err(|e| {
                    Error::Connection(format!(
                        "Failed to read directory entry: {}",
                        e
                    ))
                })?;
                let path = entry.path();

                if path.is_file() {
                    if let Ok(file) = File::open(&path) {
                        let mut reader = BufReader::new(file);
                        if let Ok(certs) = rustls_pemfile::certs(&mut reader) {
                            for cert in certs {
                                let _ = root_store.add(&Certificate(cert));
                            }
                        }
                    }
                }
            }
        }

        // Build the client config
        // Note: skip_verification is not currently supported in this rustls
        // version If you need to skip verification, consider using a
        // different TLS library or older rustls version
        let config = if let (Some(cert_path), Some(key_path)) =
            (&self.client_cert_path, &self.client_key_path)
        {
            // Mutual TLS with client certificate
            let cert_file = File::open(cert_path).map_err(|e| {
                Error::Connection(format!(
                    "Failed to open client cert {:?}: {}",
                    cert_path, e
                ))
            })?;
            let mut cert_reader = BufReader::new(cert_file);

            let certs = rustls_pemfile::certs(&mut cert_reader)
                .map_err(|e| {
                    Error::Connection(format!(
                        "Failed to parse client cert {:?}: {}",
                        cert_path, e
                    ))
                })?
                .into_iter()
                .map(Certificate)
                .collect();

            let key_file = File::open(key_path).map_err(|e| {
                Error::Connection(format!(
                    "Failed to open client key {:?}: {}",
                    key_path, e
                ))
            })?;
            let mut key_reader = BufReader::new(key_file);

            let key = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
                .map_err(|e| {
                    Error::Connection(format!(
                        "Failed to parse client key {:?}: {}",
                        key_path, e
                    ))
                })?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    Error::Connection(
                        "No private key found in key file".to_string(),
                    )
                })?;

            ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_client_auth_cert(certs, PrivateKey(key))
                .map_err(|e| {
                    Error::Connection(format!(
                        "Failed to set client auth: {}",
                        e
                    ))
                })?
        } else {
            // Standard TLS with server certificate verification
            ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_no_client_auth()
        };

        Ok(Arc::new(config))
    }
}

#[cfg(test)]
#[cfg(feature = "tls")]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn test_ssl_options_default() {
        let opts = SSLOptions::default();
        assert!(opts.use_system_certs);
        assert!(!opts.skip_verification);
        assert!(opts.use_sni);
        assert_eq!(opts.ca_cert_paths.len(), 0);
    }

    #[test]
    fn test_ssl_options_builder() {
        let opts = SSLOptions::new()
            .use_system_certs(false)
            .skip_verification(true)
            .use_sni(false)
            .server_name("example.com".to_string());

        assert!(!opts.use_system_certs);
        assert!(opts.skip_verification);
        assert!(!opts.use_sni);
        assert_eq!(opts.server_name, Some("example.com".to_string()));
    }
}
