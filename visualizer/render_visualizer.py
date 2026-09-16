"""Render a music-reactive mandala video from an audio file.

Usage:
    python visualizer/render_visualizer.py <audio> <out.mp4> \
        [--size 720] [--fps 30] [--sim 340] [--seconds N] [--assets DIR]

Frames are generated from per-frame features of the same audio that is muxed
into the output, so A/V sync is sample-accurate by construction.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

import cv2  # noqa: E402

from visualizer.audio_features import extract_features, load_audio  # noqa: E402
from visualizer.mandala_renderer import MandalaRenderer  # noqa: E402
from visualizer.nature_assets import load_textures  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("audio")
    ap.add_argument("out")
    ap.add_argument("--size", type=int, default=800)
    ap.add_argument("--fps", type=int, default=30)
    ap.add_argument("--sim", type=int, default=384)
    ap.add_argument("--seconds", type=float, default=None,
                    help="limit render to the first N seconds")
    ap.add_argument("--assets", default=os.path.join(REPO, "visualizer", "assets"))
    args = ap.parse_args()

    audio, sr = load_audio(args.audio)
    if args.seconds:
        audio = audio[: int(args.seconds * sr)]
    feats = extract_features(audio, sr, args.fps)
    print(f"[viz] {feats['n_frames']} frames @ {args.fps}fps "
          f"({feats['n_frames'] / args.fps:.1f}s), "
          f"{len(feats['beat_frames'])} beats detected")

    textures, tex_src = load_textures(args.assets, size=768)
    print(f"[viz] textures: {len(textures)} ({tex_src})")

    renderer = MandalaRenderer(size=args.size, sim=args.sim, fps=args.fps,
                               textures=textures, sr=sr)

    tmp_vid = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
    tmp_vid.close()
    writer = cv2.VideoWriter(tmp_vid.name, cv2.VideoWriter_fourcc(*"mp4v"),
                             args.fps, (args.size, args.size))
    if not writer.isOpened():
        sys.exit("could not open VideoWriter")

    t0 = time.time()
    for i in range(feats["n_frames"]):
        writer.write(renderer.render(i, feats))
        if i % (args.fps * 4) == 0 and i:
            done = i / feats["n_frames"]
            print(f"[viz] {done * 100:5.1f}%  ({time.time() - t0:5.1f}s elapsed)")
    writer.release()

    # mux the ORIGINAL audio with the rendered frames (sample-accurate sync)
    subprocess.run(
        ["ffmpeg", "-y", "-i", tmp_vid.name, "-i", args.audio,
         "-map", "0:v:0", "-map", "1:a:0",
         "-c:v", "libx264", "-crf", "18", "-preset", "medium",
         "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "192k",
         "-shortest", args.out],
        check=True, capture_output=True)
    os.unlink(tmp_vid.name)
    print(f"[viz] done in {time.time() - t0:.1f}s -> {args.out}")


if __name__ == "__main__":
    main()
