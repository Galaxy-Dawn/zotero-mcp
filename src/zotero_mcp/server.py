"""Zotero MCP server — thin entry-point that registers all tools.

The actual tool implementations live in :mod:`zotero_mcp.tools.*`.
This module re-exports public names so that existing callers
(``from zotero_mcp.server import mcp``, tests that call
``server.some_function()``, etc.) keep working.

Tool modules use module-level attribute access (e.g. ``_client.get_zotero_client()``)
so that tests can patch the canonical location directly.
"""

# -- FastMCP app instance ---------------------------------------------------
# -- Register every tool module by importing the package --------------------
import zotero_mcp.tools  # noqa: F401 — side-effect: registers all @mcp.tool
from zotero_mcp._app import mcp  # noqa: F401 — re-export

# -- Re-export client helpers (used by tests as server.X) -------------------
from zotero_mcp.client import (  # noqa: F401
    clear_active_library,
    convert_to_markdown,
    format_item_metadata,
    generate_bibtex,
    get_active_library,
    get_attachment_details,
    get_web_zotero_client,
    get_zotero_client,
    set_active_library,
)

# -- Re-export private helpers (used by tests) ------------------------------
from zotero_mcp.tools._helpers import (  # noqa: F401
    CROSSREF_TYPE_MAP,
    _attach_pdf_linked_url,
    _download_and_attach_pdf,
    _extra_has_citekey,
    _format_bbt_result,
    _format_citekey_result,
    _get_write_client,
    _handle_write_response,
    _normalize_arxiv_id,
    _normalize_doi,
    _normalize_limit,
    _normalize_str_list_input,
    _resolve_collection_names,
    _try_arxiv_from_crossref,
    _try_attach_oa_pdf,
    _try_pmc,
    _try_semantic_scholar,
    _try_unpaywall,
)
from zotero_mcp.tools.annotations import (  # noqa: F401
    _batch_resolve_parent_titles,
    _format_search_results,
    _get_annotations,
    create_annotation,
    create_note,
    get_annotations,
    get_notes,
    search_notes,
)
from zotero_mcp.tools.connectors import (  # noqa: F401
    chatgpt_connector_search,
    connector_fetch,
)
from zotero_mcp.tools.research_import import (  # noqa: F401
    add_item_by_url,
    add_items_by_arxiv,
    add_items_by_doi,
    add_items_by_identifier,
    add_linked_url_attachment,
    find_and_attach_pdfs,
    get_import_ledger,
    reconcile_collection_duplicates,
    reconcile_local_copies,
)
from zotero_mcp.tools.retrieval import (  # noqa: F401
    get_collection_items,
    get_collections,
    get_feed_items,
    get_item_children,
    get_item_fulltext,
    get_item_metadata,
    get_recent,
    get_tags,
    list_feeds,
    list_libraries,
    switch_library,
    validate_library_switch,
)

# -- Re-export tool functions (used by tests as server.func_name) -----------
from zotero_mcp.tools.search import (  # noqa: F401
    advanced_search,
    get_search_database_status,
    search_by_citation_key,
    search_by_tag,
    search_items,
    semantic_search,
    update_search_database,
)
from zotero_mcp.tools.write import (  # noqa: F401
    add_by_doi,
    add_by_url,
    add_from_file,
    batch_update_tags,
    create_collection,
    find_duplicates,
    get_pdf_outline,
    manage_collections,
    merge_duplicates,
    search_collections,
    update_item,
)
from zotero_mcp.utils import (  # noqa: F401
    clean_html,
    format_creators,
    format_item_result,
    is_local_mode,
)
