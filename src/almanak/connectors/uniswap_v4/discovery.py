"""How `almanak ax` discovers Uniswap V4 pools."""

from almanak.connectors.discovery import PoolDiscovery

# CoinGecko files launchpad pools on the Uniswap V4 PoolManager under the launchpad's
# own label; Pons pools use the reviewed PonsV2MemeHook. A label only selects the
# resolver; the PoolManager's Initialize event and the hook registry decide. Labels of
# launchpads without a reviewed hook (e.g. Bankr, Doppler) stay out: each pool would
# cost a full-history Initialize read only to be refused.
POOL = PoolDiscovery("keyed", vendor_labels=("uniswap-v4", "pons-v2-dex"))
