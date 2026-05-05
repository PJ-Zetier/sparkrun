"""Container image registry operations."""

from __future__ import annotations

import logging
import subprocess

logger = logging.getLogger(__name__)


def pull_image(image: str, dry_run: bool = False) -> int:
    """Pull a container image from a registry.

    Args:
        image: Image reference to pull (e.g. ``"nvcr.io/nvidia/vllm:latest"``).
        dry_run: If True, show what would be done without executing.

    Returns:
        Exit code (0 = success).
    """
    if dry_run:
        logger.info("[dry-run] Would pull image: %s", image)
        return 0

    logger.info("Pulling image: %s...", image)
    result = subprocess.run(
        ["docker", "pull", image],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error("Failed to pull image %s: %s", image, result.stderr[:200])
    return result.returncode


def image_exists_locally(image: str) -> bool:
    """Check if a container image exists locally.

    Args:
        image: Image reference to check.

    Returns:
        True if the image exists in the local Docker image store.
    """
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def get_image_id(image: str) -> str | None:
    """Get the Docker image ID (digest) for a local image.

    Args:
        image: Image reference to inspect.

    Returns:
        Image ID string (e.g. ``"sha256:abc123..."``) or None if the
        image does not exist locally.
    """
    result = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def get_image_size(image: str) -> int | None:
    """Get the on-disk size of a local image in bytes.

    Used to size progress bars during image distribution.  Returns
    ``None`` when the image isn't present locally or the size can't
    be parsed.
    """
    result = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Size}}", image],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    raw = result.stdout.strip()
    try:
        return int(raw)
    except ValueError:
        return None


def ensure_image(image: str, dry_run: bool = False) -> int:
    """Ensure an image exists locally, pulling if needed.

    Args:
        image: Image reference.
        dry_run: If True, show what would be done without executing.

    Returns:
        Exit code (0 = success).
    """
    if image_exists_locally(image):
        logger.info("Image already available: %s", image)
        return 0
    return pull_image(image, dry_run=dry_run)
