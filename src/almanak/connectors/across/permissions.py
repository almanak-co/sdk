"""No Roles grants yet: a Safe/Roles account cannot bridge until they are built."""

from almanak.connectors.across.authority import Capabilities, capability_codec
from almanak.gateway.permission_models import PermissionManifest

REFUSAL = "Across has no Roles grants yet; bridge from an EOA account"


def derive(profile, capabilities):
    raise ValueError(REFUSAL)


def identities(config, account, reader):
    raise ValueError(REFUSAL)


class AcrossPermissions:
    manifest_type = PermissionManifest[Capabilities]
    codec = capability_codec()
    runtime_checks = (
        "The bridge fee ceiling, the quoted output amount and the deposit deadlines are "
        "gateway checks; the SpokePool checks no price.",
    )
    derive = staticmethod(derive)
    identities = staticmethod(identities)
