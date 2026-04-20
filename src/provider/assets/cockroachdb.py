"""
CockroachDB asset URI sanitizer for Airflow.

Registers the ``cockroachdb://`` URI scheme so Airflow can recognize
CockroachDB connection URIs in asset/dataset definitions.
"""

try:
    from airflow.assets import Asset

    def sanitize_uri(uri: str) -> str:
        """
        Sanitize a CockroachDB URI by removing sensitive information
        (passwords, certificates) while preserving the host/database
        for asset identification.
        """
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(uri)

        # Remove password from netloc
        if parsed.password:
            netloc = f"{parsed.username}@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
        else:
            netloc = parsed.netloc

        # Remove sensitive query parameters
        sanitized = urlunparse((
            parsed.scheme,
            netloc,
            parsed.path,
            "",  # params
            "",  # query (remove all query params for safety)
            "",  # fragment
        ))

        return sanitized

except ImportError:
    # Airflow not installed — skip asset registration
    def sanitize_uri(uri: str) -> str:
        return uri
