"""Click parameter type for chain options: canonical choices, alias-tolerant input.

Chain identity is owned by :class:`~almanak.core.chains.ChainRegistry` —
canonical lowercase names plus per-descriptor aliases (VIB-4851). CLI surfaces
historically used ``click.Choice(cli_chain_choices())``, which advertises the
canonical names but hard-rejects registered aliases: ``almanak strat new -c bnb``
failed even though every runtime seam resolves ``bnb`` to ``bsc``
(VIB-5293 defect class).

:class:`ChainChoice` closes that gap as the single CLI ingress seam for chain
options: the advertised choices come FROM the registry (canonical names only),
any registered alias or CAIP-2 id converts to its canonical name, and an
unresolvable input fails with a message that lists the canonical choices.
Commands therefore always receive a canonical chain name — scaffolds, configs,
and downstream identity derivation never see an alias.

Lives in its own module (not ``chain_resolution.py``) because that module's
contract is "no non-stdlib dependency beyond ``almanak.core.chains``" for cheap
sweep-worker imports, and this one needs ``click``.
"""

from __future__ import annotations

import click

from almanak.core.chains import ChainRegistry
from almanak.framework.cli.chain_resolution import cli_chain_choices


class ChainChoice(click.ParamType):
    """A ``click.Choice``-alike over registry chains that accepts aliases.

    * Advertised choices (help text / metavar) are the canonical lowercase
      names from :func:`cli_chain_choices` — registry-derived, never a
      hand-maintained list.
    * ``convert`` resolves any canonical name, registered alias, or CAIP-2 id
      via :meth:`ChainRegistry.try_resolve` and returns the CANONICAL name, so
      command bodies never observe an alias.
    * ``evm_only=True`` restricts the accepted set to EVM-family chains
      (mirrors ``cli_chain_choices(evm_only=True)``); an alias of a non-EVM
      chain is rejected with the same unknown-chain error.
    """

    name = "chain"

    def __init__(self, *, evm_only: bool = False) -> None:
        self.evm_only = evm_only

    @property
    def choices(self) -> list[str]:
        """Canonical chain names, computed per call so registry changes are honoured."""
        return cli_chain_choices(evm_only=self.evm_only)

    def get_metavar(self, param: click.Parameter, ctx: click.Context | None = None) -> str:
        # Click 8.2 changed the call to ``get_metavar(param, ctx)``; 8.1 still
        # calls ``get_metavar(param)``. ``ctx`` is optional so the override
        # stays compatible across the whole ``click>=8.1.8,<9`` range
        # (VIB-5293).
        return "[" + "|".join(self.choices) + "]"

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> list[click.shell_completion.CompletionItem]:
        """Restore ``--chain`` autocompletion lost when we left ``click.Choice``.

        A custom ``ParamType`` does not inherit ``click.Choice``'s completion,
        so shells stopped suggesting chains. Completing to the canonical names
        (never aliases) keeps the advertised vocabulary aligned with
        :meth:`choices` and what a scaffolded config ends up carrying.
        """
        from click.shell_completion import CompletionItem

        return [CompletionItem(name) for name in self.choices if name.lower().startswith(incomplete.lower())]

    def convert(
        self,
        value: object,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> str | None:
        # ``None`` reaches convert only for an optional option left unset (Click
        # guards it in ``ParamType.__call__``, but keep the type robust to
        # direct/programmatic calls). Pass it through so an absent ``--chain``
        # stays absent rather than failing as an unknown chain.
        if value is None:
            return None
        choices = self.choices
        if isinstance(value, str):
            descriptor = ChainRegistry.try_resolve(value)
            if descriptor is not None and descriptor.name in choices:
                return descriptor.name
        self.fail(
            f"{value!r} is not a supported chain. Choose one of: {', '.join(choices)} "
            f"(registered chain aliases are accepted and resolve to the canonical name).",
            param,
            ctx,
        )


__all__ = ["ChainChoice"]
