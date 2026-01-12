from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

import grpc
import grpc.aio
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from loguru import logger

from symphony.config import TlsConfig


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _write_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _write_cert(path: Path, cert: x509.Certificate) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def _ensure_ca(
    ca_cert_path: Path,
    ca_key_path: Path,
    common_name: str = "symphony-ca",
    days_valid: int = 3650,
) -> None:
    if ca_cert_path.exists() and ca_key_path.exists():
        return

    logger.warning("Generating CA cert/key")

    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = _utcnow()

    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=days_valid))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=False,
                key_cert_sign=True,
                key_agreement=False,
                content_commitment=False,
                data_encipherment=False,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )

    _write_key(ca_key_path, ca_key)
    _write_cert(ca_cert_path, ca_cert)


def _ensure_signed_cert(
    *,
    cert_path: Path,
    key_path: Path,
    ca_cert_path: Path,
    ca_key_path: Path,
    common_name: str,
    san_dns: Sequence[str],
    san_ips: Sequence[str] | None = None,
    is_server: bool,
    days_valid: int = 3650,
) -> None:
    if cert_path.exists() and key_path.exists():
        return

    logger.warning("Generating signed cert {}", cert_path.name)

    ca_cert = x509.load_pem_x509_certificate(ca_cert_path.read_bytes())
    ca_key = serialization.load_pem_private_key(ca_key_path.read_bytes(), password=None)

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])

    eku = (
        [ExtendedKeyUsageOID.SERVER_AUTH]
        if is_server
        else [ExtendedKeyUsageOID.CLIENT_AUTH]
    )

    now = _utcnow()

    san_list: list[x509.GeneralName] = [x509.DNSName(d) for d in san_dns]
    if san_ips:
        from ipaddress import ip_address

        for ip in san_ips:
            try:
                san_list.append(x509.IPAddress(ip_address(ip)))
            except ValueError:
                logger.warning("Invalid IP address in SAN list: {}", ip)

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=days_valid))
        .add_extension(x509.SubjectAlternativeName(san_list), critical=False)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                key_cert_sign=False,
                key_agreement=False,
                content_commitment=False,
                data_encipherment=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage(eku), critical=False)
        .sign(ca_key, hashes.SHA256())
    )

    _write_key(key_path, key)
    _write_cert(cert_path, cert)


@dataclass(frozen=True)
class MtlsBundle:
    ca_cert: Path
    ca_key: Path
    server_cert: Path
    server_key: Path
    client_cert: Path
    client_key: Path


def ensure_mtls_bundle(cert_dir: Path, server_name: str | None = None) -> MtlsBundle:
    """
    Ensure the full mTLS bundle exists under `cert_dir`.
    Common client certs for all nodes for now
    """
    cert_dir = Path(cert_dir)

    ca_cert = cert_dir / "ca.pem"
    ca_key = cert_dir / "ca.key"

    server_cert = cert_dir / "server.pem"
    server_key = cert_dir / "server.key"

    client_cert = cert_dir / "node-client.pem"
    client_key = cert_dir / "node-client.key"

    paths = (ca_cert, ca_key, server_cert, server_key, client_cert, client_key)
    missing = [p for p in paths if not p.exists()]
    if missing:
        logger.warning(
            "TLS certificate files missing in {}: {}. "
            "Generating new self-signed certificates and keys; "
            "ensure these files are stored in persistent storage "
            "so they survive restarts. Valid for 10 years",
            cert_dir,
            ", ".join(p.name for p in missing),
        )

    _ensure_ca(ca_cert, ca_key)

    san_dns = ["localhost", "symphony-conductor"]
    san_ips: list[str] = []
    if server_name:
        # If server_name looks like an IP, add it to IP SANs;
        # otherwise, treat it as an additional DNS SAN.
        try:
            from ipaddress import ip_address

            ip_address(server_name)
            san_ips.append(server_name)
        except ValueError:
            san_dns.append(server_name)

    _ensure_signed_cert(
        cert_path=server_cert,
        key_path=server_key,
        ca_cert_path=ca_cert,
        ca_key_path=ca_key,
        common_name=server_name or "symphony-conductor",
        san_dns=tuple(san_dns),
        san_ips=tuple(san_ips) if san_ips else None,
        is_server=True,
    )

    _ensure_signed_cert(
        cert_path=client_cert,
        key_path=client_key,
        ca_cert_path=ca_cert,
        ca_key_path=ca_key,
        common_name="symphony-node-shared",
        san_dns=("symphony-node",),
        is_server=False,
    )

    return MtlsBundle(
        ca_cert=ca_cert,
        ca_key=ca_key,
        server_cert=server_cert,
        server_key=server_key,
        client_cert=client_cert,
        client_key=client_key,
    )


def build_server_credentials(cert_dir, server_name: str | None = None) -> grpc.ServerCredentials:
    bundle = ensure_mtls_bundle(cert_dir, server_name)

    return grpc.ssl_server_credentials(
        [(bundle.server_key.read_bytes(), bundle.server_cert.read_bytes())],
        root_certificates=bundle.ca_cert.read_bytes(),
        require_client_auth=True,
    )


def create_secure_channel(
    addr: str,
    tls: TlsConfig,
) -> grpc.aio.Channel:
    if not tls.ca_file or not tls.cert_file or not tls.key_file:
        raise ValueError(
            "TLS is enabled for the node, but one or more TLS paths are missing"
        )

    ca_path = Path(tls.ca_file)
    cert_path = Path(tls.cert_file)
    key_path = Path(tls.key_file)

    missing = [p for p in (ca_path, cert_path, key_path) if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "TLS file(s) not found for node client: "
            + ", ".join(str(p) for p in missing)
        )

    creds = grpc.ssl_channel_credentials(
        root_certificates=ca_path.read_bytes(),
        private_key=key_path.read_bytes(),
        certificate_chain=cert_path.read_bytes(),
    )

    return grpc.aio.secure_channel(
        addr,
        creds,
        options=[
            ("grpc.keepalive_time_ms", 20000),
            ("grpc.keepalive_timeout_ms", 5000),
        ],
    )
