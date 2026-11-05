#!/usr/bin/env python3
"""Render docs/assets/plimsoll-demo.gif from a real run of scripts/demo.sh.

Every terminal line is the demo's verbatim output, port and pid included. Nothing is simulated: the only change
is the pacing, slowed for reading, and the footer states the real duration. Needs Pillow
(`python3 -m pip install Pillow`), the DejaVu Sans Mono font, and what `make demo` needs.
    python3 docs/assets/make-demo-gif.py
"""
import os
import subprocess
import time
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs/assets/plimsoll-demo.gif"


def capture():
    start, lines = time.time(), []
    proc = subprocess.Popen([str(ROOT / "scripts/demo.sh")], cwd=ROOT, env=os.environ, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in proc.stdout:
        line = line.rstrip("\n")
        if not line.startswith("building "):
            lines.append((round(time.time() - start, 2), line))
    if proc.wait() != 0:
        raise SystemExit("scripts/demo.sh failed; no GIF written")
    return lines


LINES = capture()
FONT = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", 17)
BOLD = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf", 17)
CW, CH, PAD, TOP = 10.3, 27, 26, 58
W, H = 1120, 500
BG, BAR, FG, DIM, GREEN, AMBER, LINE = (14, 26, 43), (24, 38, 58), (232, 237, 243), (140, 154, 173), (120, 220, 150), (242, 165, 65), (242, 165, 65)
REAL = LINES[-1][0] - LINES[0][0]
NOTES = {  # what each step runs, from scripts/demo.sh
    "count 10M rows": "SELECT count(*) FROM range(10000000)",
    "group by 5M keys": "SELECT v, count(*) FROM (SELECT value % 5000000 AS v FROM range(20000000)) GROUP BY v",
    "node after the breach": "GET /readyz, GET /healthz → pool.used_bytes",
    "second query while": "a 10^10-row scan holds the only slot; then SELECT 1",
    "10^10-row scan": "SELECT sum(value % 7) FROM range(10000000000)",
    "query after all": "SELECT 'still serving'",
}


def note_for(text):
    return next((v for k, v in NOTES.items() if k in text), None)


def frame(shown, typed="", note=None):
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, W, 40], fill=BAR)
    for i, c in enumerate([(255, 95, 87), (254, 188, 46), (40, 200, 64)]):
        d.ellipse([16 + i * 22, 14, 28 + i * 22, 26], fill=c)
    d.text((W // 2, 20), "plimsoll — the query fails, the node doesn't", fill=DIM, font=FONT, anchor="mm")
    y = TOP
    d.text((PAD, y), "$ ", fill=GREEN, font=BOLD)
    d.text((PAD + 2 * CW, y), typed, fill=FG, font=BOLD)
    y += CH
    for text in shown:
        if text.strip().startswith("✓"):
            d.text((PAD, y), text[:4], fill=GREEN, font=BOLD)
            label, code, rest = text[4:43], text[43:49], text[49:]
            d.text((PAD + 4 * CW, y), label, fill=FG, font=FONT)
            d.text((PAD + 43 * CW, y), code, fill=GREEN if code.strip() == "200" else AMBER, font=BOLD)
            d.text((PAD + 49 * CW, y), rest, fill=FG if code.strip() == "200" else AMBER, font=FONT)
        elif text == "demo passed":
            d.text((PAD, y), text, fill=GREEN, font=BOLD)
        else:
            d.text((PAD, y), text, fill=DIM if text.strip().startswith("/metrics") else FG, font=FONT)
        y += CH
    if note:
        d.rectangle([PAD - 8, H - 84, W - PAD + 8, H - 50], outline=LINE, width=1)
        d.text((PAD, H - 67), "SQL ", fill=AMBER, font=BOLD, anchor="lm")
        d.text((PAD + 4 * CW, H - 67), note, fill=DIM, font=FONT, anchor="lm")
    d.line([PAD, H - 36, W - PAD, H - 36], fill=(40, 56, 80))
    d.text((PAD, H - 20), f"real output of scripts/demo.sh · real run {REAL:.1f} s, paced for reading", fill=DIM, font=FONT, anchor="lm")
    d.text((W - PAD, H - 20), "github.com/Joaolucas2210/plimsoll", fill=DIM, font=FONT, anchor="rm")
    return img


frames, durations = [], []


def add(img, ms):
    frames.append(img)
    durations.append(ms)


cmd = "make demo"
add(frame([]), 700)
for i in range(1, len(cmd) + 1):
    add(frame([], cmd[:i]), 70)
add(frame([], cmd), 500)
shown = []
for _t, text in LINES:
    shown.append(text)
    note = note_for(text)
    if not text.strip():
        continue
    hold = 2600 if "crosses the line" in text else 1900 if note else 900
    add(frame(shown, cmd, note), hold)
add(frame(shown, cmd), 4200)

frames[0].save(OUT, save_all=True, append_images=frames[1:], duration=durations,
               loop=0, optimize=True, disposal=1)
print(f"{len(frames)} frames, {sum(durations) / 1000:.1f} s, {OUT.stat().st_size // 1024} KiB")
