"""Maps an environment ("prod" or "dev") to the Unity Catalog names the pipeline reads and writes (pure logic).

Production keeps the original names (`workspace.bronze.*`, volume `workspace.landing.raw`). Development uses the same
catalog with every schema prefixed by `dev_` (`workspace.dev_bronze.*`, volume `workspace.dev_landing.raw`). This is
isolation by naming inside one catalog: it prevents accidental writes, it is not an access control.
"""

import re
from dataclasses import dataclass

LAYERS = ("landing", "bronze", "silver", "gold", "ops")
PREFIXES = {"prod": "", "dev": "dev_"}
DEV_PREFIX = PREFIXES["dev"]
_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass(frozen=True)
class Namespace:
    catalog: str
    environment: str

    @property
    def prefix(self) -> str:
        return PREFIXES[self.environment]

    def schema(self, layer: str) -> str:
        """Fully qualified schema name, e.g. `workspace.dev_silver`."""
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
        return f"{self.catalog}.{self.prefix}{layer}"

    def table(self, layer: str, name: str) -> str:
        """Fully qualified table name, e.g. `workspace.dev_silver.trips`."""
        if not _NAME_RE.match(name or ""):
            raise ValueError(f"bad table name {name!r}")
        return f"{self.schema(layer)}.{name}"

    @property
    def volume_root(self) -> str:
        """Root path of the landing volume (`/Volumes/<catalog>/<schema>/raw`)."""
        return f"/Volumes/{self.catalog}/{self.prefix}landing/raw"


def resolve(catalog: str, environment: str) -> Namespace:
    """Validate the two values and return the namespace; anything unexpected raises ValueError."""
    if not _NAME_RE.match(catalog or ""):
        raise ValueError(f"bad catalog {catalog!r}")
    if environment not in PREFIXES:
        raise ValueError(f"environment must be one of {sorted(PREFIXES)}, got {environment!r}")
    ns = Namespace(catalog, environment)
    if environment == "dev":
        # Guard: a dev namespace must never resolve to a production schema or the production volume.
        for layer in LAYERS:
            if not ns.schema(layer).split(".")[1].startswith(DEV_PREFIX):
                raise ValueError(f"dev schema for {layer!r} is not prefixed with {DEV_PREFIX!r}")
    return ns


def render_sql(text: str, catalog: str, environment: str) -> str:
    """Rewrite production names in DDL text (`catalog.layer`, `/Volumes/catalog/layer/`) for `environment`."""
    prefix = resolve(catalog, environment).prefix
    layers = "|".join(LAYERS)
    qualified = re.compile(rf"(?<![\w.]){re.escape(catalog)}\.({layers})\b")
    volume = re.compile(rf"/Volumes/{re.escape(catalog)}/({layers})/")
    text = qualified.sub(lambda m: f"{catalog}.{prefix}{m.group(1)}", text)
    return volume.sub(lambda m: f"/Volumes/{catalog}/{prefix}{m.group(1)}/", text)
