"""Gated federal-scope modules (isolated from the GO runtime).

This package preserves code that was originally written for a national
(multi-state, federal) scope. The Fiscal Cidadao product is currently
Goias-only, so these routers and queries are **not** wired into the
main FastAPI app by default.

Set ``ENABLE_FEDERAL_ROUTES=true`` in the environment to have
``bracc.main`` load and mount these routers at startup. See
``docs/_federal_gating.md`` for details and pre-requisites (e.g.
installing the optional ``federal`` extras when they are added).

Nothing here is imported automatically; adding a new module to this
package should not affect the GO runtime.
"""
