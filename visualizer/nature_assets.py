"""Nature textures for the mandala visualizer.

If the user drops real photos into ``visualizer/assets/`` those are used;
otherwise three procedural nature textures (fern, leaf, flower) are generated
so the visualizer works out of the box. All textures are BGR uint8 squares.
"""
from __future__ import annotations

import glob
import os

import cv2
import numpy as np


def _organic_noise(size, sigma, rng):
    n = rng.random((size, size))
    n = cv2.GaussianBlur(n, (0, 0), sigma)
    n = (n - n.min()) / (n.max() - n.min() + 1e-9)
    return n


def _colorize(height, stops):
    """Map a 0..1 height map through colour stops [(pos, (b,g,r)), ...]."""
    h = np.clip(height, 0.0, 1.0)
    out = np.zeros((*h.shape, 3), dtype=np.float64)
    for (p0, c0), (p1, c1) in zip(stops[:-1], stops[1:]):
        seg = np.clip((h - p0) / (p1 - p0 + 1e-9), 0.0, 1.0)
        mask = (h >= p0) & (h <= p1 + 1e-9)
        for ch in range(3):
            out[..., ch] = np.where(mask, c0[ch] + seg * (c1[ch] - c0[ch]),
                                    out[..., ch])
    return np.clip(out, 0, 255).astype(np.uint8)


def fern_texture(size=768, seed=3):
    """Barnsley-fern point cloud on mossy ground."""
    rng = np.random.default_rng(seed)
    pts = np.zeros((size, size))
    x, y = 0.0, 0.0
    coeffs = [
        (0.01, (0.0, 0.0, 0.0, 0.16, 0.0, 0.0)),
        (0.86, (0.85, 0.04, -0.04, 0.85, 0.0, 1.6)),
        (0.93, (0.20, -0.26, 0.23, 0.22, 0.0, 1.6)),
        (1.00, (-0.15, 0.28, 0.26, 0.24, 0.0, 0.44)),
    ]
    for _ in range(90000):
        r = rng.random()
        for p, (a, b, c, d, e, f) in coeffs:
            if r <= p:
                x, y = a * x + b * y + e, c * x + d * y + f
                break
        px = int((x + 2.7) / 5.4 * (size - 1))
        py = int((1.0 - y / 10.2) * (size - 1))
        if 0 <= px < size and 0 <= py < size:
            pts[py, px] += 1.0
    pts = cv2.GaussianBlur(pts, (0, 0), 1.2)
    pts = np.clip(pts / (np.percentile(pts, 99.5) + 1e-9), 0, 1) ** 0.6
    ground = _organic_noise(size, 22, rng) * 0.35
    h = np.clip(ground + pts, 0, 1)
    img = _colorize(h, [
        (0.00, (12, 22, 8)), (0.30, (20, 60, 18)),
        (0.65, (40, 150, 60)), (1.00, (150, 250, 190)),
    ])
    tex = _organic_noise(size, 2.2, rng)
    return (img.astype(np.float64) * (0.82 + 0.36 * tex[..., None])).clip(0, 255).astype(np.uint8)


def leaf_texture(size=768, seed=11):
    """Overlapping autumn leaves with veins."""
    rng = np.random.default_rng(seed)
    h = _organic_noise(size, 26, rng) * 0.25
    yy, xx = np.mgrid[0:size, 0:size].astype(np.float64)
    for _ in range(26):
        cx, cy = rng.uniform(0.1, 0.9, 2) * size
        ang = rng.uniform(0, 2 * np.pi)
        sc = rng.uniform(0.10, 0.22) * size
        dx, dy = xx - cx, yy - cy
        u = (dx * np.cos(ang) + dy * np.sin(ang)) / sc
        v = (-dx * np.sin(ang) + dy * np.cos(ang)) / (sc * 0.55)
        rr = np.sqrt(u ** 2 + v ** 2)
        th = np.arctan2(v, u)
        outline = (1.0 + 0.9 * np.cos(th)) * (1.0 + 0.04 * np.cos(th * 12))
        body = np.clip(1.0 - rr / (outline * 0.5 + 1e-9), 0, 1) ** 0.5
        vein = (np.abs(np.sin(th * 7)) ** 8) * body * 0.5
        h = np.maximum(h, body * rng.uniform(0.55, 1.0) - vein * 0.25)
    img = _colorize(np.clip(h, 0, 1), [
        (0.00, (10, 18, 26)), (0.30, (20, 55, 120)),
        (0.62, (30, 120, 215)), (1.00, (120, 235, 255)),
    ])
    tex = _organic_noise(size, 2.5, rng)
    return (img.astype(np.float64) * (0.85 + 0.30 * tex[..., None])).clip(0, 255).astype(np.uint8)


def flower_texture(size=768, seed=27):
    """Layered rose-curve petals."""
    rng = np.random.default_rng(seed)
    c = size / 2.0
    yy, xx = np.mgrid[0:size, 0:size].astype(np.float64)
    rr = np.sqrt((xx - c) ** 2 + (yy - c) ** 2) / (size * 0.5)
    th = np.arctan2(yy - c, xx - c)
    h = _organic_noise(size, 30, rng) * 0.2
    for k, scale, w in [(5, 0.95, 0.9), (7, 0.62, 1.0), (9, 0.34, 1.1)]:
        petal = np.abs(np.cos(th * k / 2.0)) ** 1.5 * scale
        body = np.clip(1.0 - np.abs(rr - petal * 0.75) / (0.28 * scale), 0, 1) ** 1.4
        h = np.maximum(h, body * w * np.clip(1.15 - rr, 0, 1))
    h = np.maximum(h, np.clip(1.0 - rr / 0.16, 0, 1))  # centre disc
    img = _colorize(np.clip(h, 0, 1), [
        (0.00, (30, 8, 25)), (0.35, (110, 30, 120)),
        (0.68, (190, 90, 235)), (1.00, (200, 235, 255)),
    ])
    tex = _organic_noise(size, 2.0, rng)
    return (img.astype(np.float64) * (0.85 + 0.30 * tex[..., None])).clip(0, 255).astype(np.uint8)


def load_textures(assets_dir, size=768):
    """User photos from assets_dir if present, else procedural nature set."""
    imgs = []
    for pattern in ("*.jpg", "*.jpeg", "*.png", "*.webp"):
        for p in sorted(glob.glob(os.path.join(assets_dir, pattern))):
            im = cv2.imread(p, cv2.IMREAD_COLOR)
            if im is None:
                continue
            s = min(im.shape[:2])
            y0 = (im.shape[0] - s) // 2
            x0 = (im.shape[1] - s) // 2
            imgs.append(cv2.resize(im[y0:y0 + s, x0:x0 + s], (size, size),
                                   interpolation=cv2.INTER_AREA))
    if imgs:
        return imgs, "user assets"
    return [fern_texture(size), leaf_texture(size), flower_texture(size)], "procedural"
