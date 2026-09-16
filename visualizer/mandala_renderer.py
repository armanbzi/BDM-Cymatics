"""Music-reactive mandala renderer.

Keeps the cymatics DNA: the *unmodified* shared engine supplies the Chladni
interference field, which both shades the image (nodal filigree) and warps the
nature textures. A kaleidoscope fold adds N-way mandala symmetry, and every
motion parameter is driven by the per-frame audio features:

    bass  -> mandala breathing / zoom punch-in
    kick  -> spin burst, white flash, chromatic aberration
    mid   -> twist / swirl amount
    high  -> glow on the nodal ridges
    pitch -> colour palette (low warm -> high cool)
    strong onsets -> symmetry + texture changes

A final grade (bloom + saturation/contrast + unsharp) gives it the luminous
"pro" finish.
"""
from __future__ import annotations

import colorsys

import cv2
import numpy as np

from shared.cymatics_engine import (
    N_ZONES, build_zones, build_zone_sources, analyse_frame,
    compute_interference, displacement_to_brightness,
)
from visualizer.audio_features import frame_chunk

SYMMETRIES = [6, 8, 5, 10, 7]


def _palette(hue):
    """(mid, highlight) colours in BGR float 0..255 for a given hue 0..1."""
    def bgr(h, s, v):
        r, g, b = colorsys.hsv_to_rgb(h % 1.0, s, v)
        return np.array([b * 255.0, g * 255.0, r * 255.0])
    return bgr(hue, 0.75, 0.55), bgr(hue + 0.06, 0.28, 1.0)


class MandalaRenderer:
    def __init__(self, size=800, sim=384, fps=30, textures=None, sr=44100, seed=7):
        self.S, self.G, self.fps, self.sr = size, sim, fps, sr
        self.textures = textures or []
        self.rng = np.random.default_rng(seed)

        c = size / 2.0
        yy, xx = np.mgrid[0:size, 0:size].astype(np.float64)
        self.r_px = np.sqrt((xx - c) ** 2 + (yy - c) ** 2)
        self.theta = np.arctan2(yy - c, xx - c)
        self.rn = np.clip(self.r_px / (size / 2.0), 0.0, 1.5)
        self.vignette = np.clip(1.0 - (self.r_px / (size * 0.72)) ** 4, 0.0, 1.0)

        self.zones = build_zones(sim)
        self.sources = build_zone_sources(sim, self.zones)
        self.zmasks = [m.astype(np.float64) for m in self.zones["masks"]]

        # motion / section state
        self.rot = 0.0
        self.spin_dir = 1.0
        self.swirl_phase = 0.0
        self.tex_rot = 0.0
        self.sym_i = 0
        self.tex_i = 0
        self.tex_next = 1 % max(1, len(self.textures))
        self.tex_fade = -1.0            # <0 = not fading
        self.last_sym_f = -10 ** 9
        self.last_tex_f = -10 ** 9
        self.br_s = None
        self.dx_s = None
        self.dy_s = None

    # ------------------------------------------------------------------
    def _cymatics(self, feats, i, t):
        chunk = frame_chunk(feats, i)
        br = np.zeros((self.G, self.G))
        dxt = np.zeros((self.G, self.G))
        dyt = np.zeros((self.G, self.G))
        for zi in range(N_ZONES):
            rms, sfreqs, _ = analyse_frame(chunk, zi, self.sr)
            dx, dy = compute_interference(self.sources[zi], sfreqs, t, self.G)
            bz = displacement_to_brightness(dx, dy, self.zones["masks"][zi],
                                            self.G, rms, t)
            br += bz * self.zmasks[zi]
            dxt += dx * self.zmasks[zi]
            dyt += dy * self.zmasks[zi]
        if self.br_s is None:
            self.br_s, self.dx_s, self.dy_s = br, dxt, dyt
        else:
            self.br_s = 0.55 * self.br_s + 0.45 * br
            self.dx_s = 0.55 * self.dx_s + 0.45 * dxt
            self.dy_s = 0.55 * self.dy_s + 0.45 * dyt
        brn = cv2.resize(self.br_s, (self.S, self.S), interpolation=cv2.INTER_CUBIC)
        brn = brn / (brn + 0.55)
        brc = np.clip(brn * 1.55, 0.0, 1.0)
        dxr = cv2.resize(self.dx_s, (self.S, self.S), interpolation=cv2.INTER_LINEAR)
        dyr = cv2.resize(self.dy_s, (self.S, self.S), interpolation=cv2.INTER_LINEAR)
        sd = np.std(dxr) + np.std(dyr) + 1e-6
        return brc, dxr / sd, dyr / sd

    # ------------------------------------------------------------------
    def _sample_nature(self, tex, a2, rr_px, dxr, dyr, warp):
        T = tex.shape[0]
        tsc = (T * 0.92) / self.S
        tx = T / 2.0 + rr_px * tsc * np.cos(a2 + self.tex_rot) + dxr * warp
        ty = T / 2.0 + rr_px * tsc * np.sin(a2 + self.tex_rot) + dyr * warp
        return cv2.remap(tex, tx.astype(np.float32), ty.astype(np.float32),
                         cv2.INTER_LINEAR, borderMode=cv2.BORDER_REFLECT_101)

    def _chromatic_aberration(self, img, amount):
        h, w = img.shape[:2]
        b, g, r = cv2.split(img)
        def scaled(ch, s):
            m = cv2.getRotationMatrix2D((w / 2.0, h / 2.0), 0.0, s)
            return cv2.warpAffine(ch, m, (w, h), flags=cv2.INTER_LINEAR,
                                  borderMode=cv2.BORDER_REFLECT_101)
        return cv2.merge([scaled(b, 1.0 - amount), g, scaled(r, 1.0 + amount)])

    def _bloom(self, img, strength=0.6):
        """Additive multi-scale bloom off the bright regions."""
        f = img.astype(np.float32)
        bright = np.clip((f - 145.0) / 110.0, 0.0, 1.0) * f
        b1 = cv2.GaussianBlur(bright, (0, 0), self.S / 55.0)
        b2 = cv2.GaussianBlur(bright, (0, 0), self.S / 20.0)
        out = f + (0.55 * b1 + 0.65 * b2) * strength
        return np.clip(out, 0.0, 255.0).astype(np.uint8)

    def _grade(self, img, sat=1.38, contrast=1.14):
        """Saturation boost + contrast S-curve — fixes muddy/greyed frames."""
        hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV).astype(np.float32)
        hsv[..., 1] = np.clip(hsv[..., 1] * sat, 0.0, 255.0)
        rgb = cv2.cvtColor(hsv.astype(np.uint8), cv2.COLOR_HSV2BGR).astype(np.float32) / 255.0
        rgb = np.clip((rgb - 0.5) * contrast + 0.5, 0.0, 1.0)
        rgb = rgb ** 0.94
        return (rgb * 255.0).astype(np.uint8)

    def _unsharp(self, img, amount=0.55):
        blur = cv2.GaussianBlur(img, (0, 0), self.S / 380.0)
        return cv2.addWeighted(img, 1.0 + amount, blur, -amount, 0)

    # ------------------------------------------------------------------
    def render(self, i, feats):
        fps = self.fps
        dt = 1.0 / fps
        t = i / fps
        bass = feats["bass"][i]; mid = feats["mid"][i]; high = feats["high"][i]
        loud = feats["loud"][i]; kick = feats["kick"][i]; onset = feats["onset"][i]
        hue = feats["hue"][i]

        # --- integrate motion (music drives the derivatives) -------------
        self.rot += dt * (0.10 + 0.55 * loud + 1.6 * kick) * self.spin_dir
        self.swirl_phase += dt * (0.9 + 3.2 * mid)
        self.tex_rot += dt * 0.045

        if feats["strong_onset"][i]:
            if i - self.last_sym_f > 4 * fps:
                self.sym_i = (self.sym_i + 1) % len(SYMMETRIES)
                if self.rng.random() < 0.35:
                    self.spin_dir *= -1.0
                self.last_sym_f = i
            if len(self.textures) > 1 and self.tex_fade < 0 \
                    and i - self.last_tex_f > 8 * fps:
                self.tex_next = (self.tex_i + 1) % len(self.textures)
                self.tex_fade = 0.0
                self.last_tex_f = i

        # --- cymatics field (co-rotated with the mandala) ---------------
        brc, dxr, dyr = self._cymatics(feats, i, t)
        deg = self.rot * 180.0 / np.pi
        m_rot = cv2.getRotationMatrix2D((self.S / 2.0, self.S / 2.0), deg, 1.0)
        brc = cv2.warpAffine(brc, m_rot, (self.S, self.S), flags=cv2.INTER_LINEAR,
                             borderMode=cv2.BORDER_REFLECT_101)

        # --- kaleidoscope fold + music-driven warp ----------------------
        n_sym = SYMMETRIES[self.sym_i]
        wedge = 2.0 * np.pi / n_sym
        a = np.mod(self.theta + self.rot, wedge)
        a = np.abs(a - wedge / 2.0)
        twist = (0.10 + 0.50 * mid + 0.20 * onset)
        a2 = a + twist * np.sin(self.rn * 2.6 * np.pi - self.swirl_phase) \
            * (0.30 + 0.70 * self.rn)
        zoom = 1.06 - 0.20 * bass - 0.10 * kick
        rr_px = self.r_px * zoom
        warp = 2.0 + 9.0 * mid

        tex = self.textures[self.tex_i]
        nat = self._sample_nature(tex, a2, rr_px, dxr, dyr, warp)
        if self.tex_fade >= 0.0:
            nat_b = self._sample_nature(self.textures[self.tex_next],
                                        a2, rr_px, dxr, dyr, warp)
            f = min(self.tex_fade, 1.0)
            nat = cv2.addWeighted(nat, 1.0 - f, nat_b, f, 0)
            self.tex_fade += dt / 1.2
            if self.tex_fade >= 1.0:
                self.tex_i, self.tex_fade = self.tex_next, -1.0

        # --- compose: nature carved by the nodal field -------------------
        mid_c, hi_c = _palette(hue)
        col = nat.astype(np.float32) / 255.0
        shade = (0.26 + 0.74 * brc ** 0.85).astype(np.float32)
        col *= shade[..., None]
        col *= (0.72 + 0.28 * mid_c / 255.0).astype(np.float32)[None, None, :]
        ridge = (brc ** 3 * (0.30 + 0.70 * high)).astype(np.float32)
        col += (hi_c / 255.0).astype(np.float32)[None, None, :] * ridge[..., None] * 0.9
        col += np.float32(0.13 * kick)
        col = np.clip(col, 0.0, 1.0)
        img = (col * 255.0).astype(np.uint8)

        if kick > 0.25:
            img = self._chromatic_aberration(img, 0.006 * kick)

        # --- luminous grade ---------------------------------------------
        img = self._bloom(img, strength=0.5 + 0.45 * high + 0.30 * kick)
        img = self._grade(img, sat=1.38, contrast=1.14)
        img = self._unsharp(img, amount=0.55)
        img = (img.astype(np.float32) * self.vignette[..., None]).astype(np.uint8)
        return img
