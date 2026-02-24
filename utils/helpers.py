"""
utils/helpers.py
----------------
Video assembly from screenshots + Cloudinary upload.

Guarantees:
  - Video is always produced (even 1 frame → 3-second still video)
  - Cloudinary upload is attempted; falls back to local path on failure
  - Returns a string URL no matter what
"""

from __future__ import annotations

import asyncio
import glob
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import cloudinary
import cloudinary.uploader


# ---------------------------------------------------------------------------
# Cloudinary config (reads from environment)
# ---------------------------------------------------------------------------

def _init_cloudinary() -> bool:
    """Configure cloudinary from env vars. Returns True if all vars present."""
    cloud_name = os.getenv("CLOUDINARY_CLOUD_NAME")
    api_key    = os.getenv("CLOUDINARY_API_KEY")
    api_secret = os.getenv("CLOUDINARY_API_SECRET")

    if not all([cloud_name, api_key, api_secret]):
        print("[Cloudinary] Missing env vars — upload will be skipped.")
        return False

    cloudinary.config(
        cloud_name=cloud_name,
        api_key=api_key,
        api_secret=api_secret,
        secure=True,
    )
    return True


# ---------------------------------------------------------------------------
# Frame helpers
# ---------------------------------------------------------------------------

def _get_valid_frames(frames_dir: str) -> list[str]:
    """
    Return sorted list of PNGs that are:
      - Actually PNG files (not .txt concat lists accidentally named .png)
      - Larger than 1×1 pixel (i.e., real screenshots, not stub placeholders)

    Tiny placeholder frames (1×1) are upscaled in the ffmpeg step, so we
    still include them — we just warn about them.
    """
    all_pngs = sorted(glob.glob(os.path.join(frames_dir, "*.png")))
    valid    = []

    for p in all_pngs:
        try:
            from PIL import Image
            with Image.open(p) as img:
                w, h = img.size
                valid.append(p)
                if w <= 4 or h <= 4:
                    print(f"[Frames] ⚠️  Tiny frame ({w}×{h}): {p} — will upscale")
        except Exception as exc:
            print(f"[Frames] Skipping unreadable PNG {p}: {exc}")

    return valid


def _preprocess_frame(src: str, dst: str, target_w: int = 1920, target_h: int = 1080) -> None:
    """
    Copy *src* to *dst*, resizing to *target_w*×*target_h* if needed.
    Ensures even dimensions (libx264 requirement).
    Uses Pillow — always available given our dependencies.
    """
    from PIL import Image

    with Image.open(src) as img:
        img = img.convert("RGB")
        w, h = img.size

        # Upscale tiny placeholders to a real size
        if w <= 4 or h <= 4:
            canvas = Image.new("RGB", (target_w, target_h), (30, 30, 30))
            canvas.save(dst, "PNG")
            return

        # Ensure even dimensions (libx264 requirement)
        nw = w if w % 2 == 0 else w - 1
        nh = h if h % 2 == 0 else h - 1
        if nw != w or nh != h:
            img = img.crop((0, 0, nw, nh))

        img.save(dst, "PNG")


# ---------------------------------------------------------------------------
# ffmpeg helpers
# ---------------------------------------------------------------------------

def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def _build_video_ffmpeg(
    frames_dir: str,
    output_path: str,
    fps: int = 2,
) -> bool:
    """
    Use ffmpeg (concat demuxer) to stitch PNGs → MP4.
    Pre-processes each frame with Pillow to guarantee valid dimensions.
    Returns True on success.
    """
    frames = _get_valid_frames(frames_dir)
    if not frames:
        print("[ffmpeg] No valid PNG frames found.")
        return False

    print(f"[ffmpeg] Stitching {len(frames)} frame(s) at {fps} fps → {output_path}")

    with tempfile.TemporaryDirectory() as tmp:
        # Pre-process all frames into the temp dir
        processed = []
        for i, src in enumerate(frames):
            dst = os.path.join(tmp, f"frame_{i:06d}.png")
            try:
                _preprocess_frame(src, dst)
                processed.append(dst)
            except Exception as exc:
                print(f"[ffmpeg] Skipping frame {src}: {exc}")

        if not processed:
            print("[ffmpeg] No frames survived preprocessing.")
            return False

        # Write ffmpeg concat list
        concat_file = os.path.join(tmp, "concat.txt")
        with open(concat_file, "w") as f:
            for p in processed:
                f.write(f"file '{p}'\n")
                f.write(f"duration {1/fps:.4f}\n")
            # Repeat last frame so ffmpeg knows the final duration
            f.write(f"file '{processed[-1]}'\n")

        cmd = [
            "ffmpeg", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", concat_file,
            # scale to even 1920×1080, pad if needed, keep SAR clean
            "-vf", (
                "scale=1920:1080:force_original_aspect_ratio=decrease,"
                "pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=black,"
                "setsar=1"
            ),
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-crf", "23",
            output_path,
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode != 0:
                print(f"[ffmpeg] Error:\n{result.stderr[-3000:]}")
                return False

            size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
            if size == 0:
                print("[ffmpeg] Output file is 0 bytes — encoding failed silently.")
                return False

            print(f"[ffmpeg] ✅ Video written: {output_path} ({size:,} bytes)")
            return True

        except subprocess.TimeoutExpired:
            print("[ffmpeg] Timed out after 300s.")
            return False
        except Exception as exc:
            print(f"[ffmpeg] Unexpected error: {exc}")
            return False


def _build_video_pillow_gif(
    frames_dir: str,
    output_path: str,
    fps: int = 2,
) -> bool:
    """
    Pure-Pillow animated GIF fallback (no ffmpeg, no imageio needed).
    Returns True on success.
    """
    frames = _get_valid_frames(frames_dir)
    if not frames:
        return False

    # Force .gif extension
    gif_path = str(Path(output_path).with_suffix(".gif"))
    print(f"[GIF] Building animated GIF from {len(frames)} frame(s) → {gif_path}")

    try:
        from PIL import Image

        images = []
        for p in frames:
            try:
                img = Image.open(p).convert("RGB")
                # Downscale for GIF to keep file size manageable
                img.thumbnail((960, 540), Image.LANCZOS)
                images.append(img)
            except Exception as exc:
                print(f"[GIF] Skipping frame {p}: {exc}")

        if not images:
            return False

        duration_ms = int(1000 / fps)
        images[0].save(
            gif_path,
            save_all=True,
            append_images=images[1:],
            loop=0,
            duration=duration_ms,
            optimize=False,
        )

        size = os.path.getsize(gif_path)
        print(f"[GIF] ✅ Written: {gif_path} ({size:,} bytes)")
        return True

    except Exception as exc:
        print(f"[GIF] Failed: {exc}")
        return False


# ---------------------------------------------------------------------------
# Cloudinary upload
# ---------------------------------------------------------------------------

def _upload_to_cloudinary(local_path: str, public_id: str) -> Optional[str]:
    """Upload file to Cloudinary. Returns secure_url or None."""
    if not _init_cloudinary():
        return None

    ext           = Path(local_path).suffix.lower()
    resource_type = "video" if ext in (".mp4", ".mov", ".webm", ".gif") else "image"

    print(f"[Cloudinary] Uploading {local_path} as {resource_type}…")
    try:
        response = cloudinary.uploader.upload(
            local_path,
            public_id=f"browser_agent/{public_id}",
            resource_type=resource_type,
            overwrite=True,
        )
        url = response.get("secure_url")
        print(f"[Cloudinary] ✅ Uploaded → {url}")
        return url
    except Exception as exc:
        print(f"[Cloudinary] Upload failed: {exc}")
        return None


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

async def create_and_upload_video(
    folder: str,
    session_id: str,
    fps: int = 2,
) -> Optional[str]:
    """
    1. Stitch all PNGs in *folder* into a video (MP4 via ffmpeg, GIF fallback).
    2. Upload to Cloudinary.
    3. Return the Cloudinary secure URL (or local path as last resort).

    Never raises — always returns a string or None.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_create_and_upload, folder, session_id, fps)


def _sync_create_and_upload(folder: str, session_id: str, fps: int) -> Optional[str]:
    """Synchronous implementation called from executor."""

    video_path = os.path.join(folder, f"{session_id}.mp4")
    success    = False

    # ── 1. Try ffmpeg (MP4) ───────────────────────────────────────────────
    if _ffmpeg_available():
        success = _build_video_ffmpeg(folder, video_path, fps=fps)
    else:
        print("[Video] ffmpeg not in PATH — skipping MP4.")

    # ── 2. Fallback: pure-Pillow animated GIF ────────────────────────────
    if not success:
        gif_path = os.path.join(folder, f"{session_id}.gif")
        success  = _build_video_pillow_gif(folder, gif_path, fps=fps)
        if success:
            video_path = gif_path

    # ── 3. Last resort: upload first PNG as a still image ─────────────────
    if not success:
        frames = _get_valid_frames(folder)
        if frames:
            video_path = frames[0]
            print(f"[Video] Using first frame as still fallback: {video_path}")
            success = True

    if not success:
        print("[Video] ⚠️  Nothing to upload.")
        return None

    # ── 4. Upload to Cloudinary ───────────────────────────────────────────
    url = _upload_to_cloudinary(video_path, session_id)
    if url:
        return url

    print(f"[Video] Cloudinary unavailable — returning local path: {video_path}")
    return video_path