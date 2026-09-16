# Music-Reactive Mandala Visualizer

Turns an audio file into a kaleidoscope-mandala video whose motion, colour, and
detail are driven frame-by-frame by the music. Built on top of the project's
cymatics engine (`shared/cymatics_engine.py`, imported **read-only**): the
Chladni interference field both shades the nodal filigree and warps the nature
imagery, folded into N-way mandala symmetry.

Sync is sample-accurate — features are computed on the exact per-video-frame
audio window, and the original audio is muxed back into the output.

## Usage

```bash
python visualizer/render_visualizer.py <audio> <out.mp4> [options]
#   --size 800   output resolution (square)
#   --fps 30     frames per second
#   --sim 384    cymatics simulation grid (detail vs speed)
#   --seconds N  limit to the first N seconds
#   --assets DIR nature-image folder (default: visualizer/assets)
```

`<audio>` is any format ffmpeg can decode (wav / mp3 / m4a / flac / ...).

## Nature imagery

Drop square-ish photos (`.jpg/.png/.webp`) into `visualizer/assets/` and they
are used as the mandala texture. With an empty folder, three procedural
textures (fern / leaf / flower) are generated automatically, so it works out of
the box.

## Reactive mapping

| Audio           | Visual                                    |
|-----------------|-------------------------------------------|
| bass            | mandala breathing / zoom punch            |
| kick            | spin burst + flash + chromatic aberration |
| mids            | twist / swirl amount                      |
| highs           | glow on the nodal ridges                  |
| dominant pitch  | colour palette (low warm → high cool)     |
| strong onsets   | symmetry + texture changes                |

## Files

- `render_visualizer.py` — CLI entrypoint (audio → mp4)
- `audio_features.py` — per-frame feature extraction (bands, kick/onset, pitch)
- `mandala_renderer.py` — kaleidoscope fold + cymatics warp + luminous grade
- `nature_assets.py` — procedural textures / user-photo loader

## Dependencies

`numpy`, `scipy`, `opencv-python`, and `ffmpeg` — all already used by the
project. `librosa` is **optional**: used for beat tracking when installed, with
a spectral-flux fallback otherwise.
