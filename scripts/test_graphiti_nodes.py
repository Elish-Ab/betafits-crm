"""Utility to exercise GraphitiClient.get_nodes_by_namespace."""

import argparse
import asyncio
import logging
from typing import Iterable

from lib.integrations.graphiti_client import get_graphiti_client

logger = logging.getLogger(__name__)


def _parse_labels(value: str) -> list[str]:
    return [label.strip() for label in value.split(",") if label.strip()]


async def _run(group_id: str, node_labels: Iterable[str]) -> None:
    client = get_graphiti_client()
    try:
        nodes = await client.get_nodes_by_namespace(list(node_labels), group_id)
        if not nodes:
            logger.info("No nodes found for group_id %s", group_id)
            return
        logger.info("Found %d nodes", len(nodes))
        for node in nodes:
            logger.info("node=%s labels=%s", node.uuid, node.labels)
            print(
                f"uuid={node.uuid} name={node.name} labels={node.labels} group_id={node.group_id}"
            )
    finally:
        # await client.close()
        pass


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Test GraphitiClient.get_nodes_by_namespace"
    )
    parser.add_argument("--group-id", required=True, help="Namespace/group identifier")
    parser.add_argument(
        "--labels",
        default="Opportunity",
        help="Comma-separated node labels to filter (default: Opportunity)",
        type=_parse_labels,
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args.group_id, args.labels))


if __name__ == "__main__":
    main()
