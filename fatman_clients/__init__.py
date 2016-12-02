

from os import path

CA_BUNDLE_SEARCH_PATHS = [
    "/etc/ssl/ca-bundle.pem",  # OpenSUSE
    "/etc/ssl/certs/ca-certificates.crt",  # Gentoo
    ]


def try_verify_by_system_ca_bundle():
    """Try to locate a system CA bundle and use that if available,
       otherwise, return True to use the bundled (provided by certifi) CA package"""

    for ca_path in CA_BUNDLE_SEARCH_PATHS:
        if path.exists(ca_path):
            return ca_path

    # Return None to fallback to the Python-Requests Session default
    return None
