"""Render the high-level architecture diagram to PNG and SVG.

Run:  python .planning/make_architecture_png.py
Outputs: .planning/architecture.png  and  .planning/architecture.svg
"""

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

HERE = os.path.dirname(os.path.abspath(__file__))

FONT = "DejaVu Sans"

fig, ax = plt.subplots(figsize=(9, 13))
ax.set_xlim(0, 10)
ax.set_ylim(0, 15)
ax.axis("off")

TITLE = "Real-Time IoT Streaming Medallion Pipeline (Databricks)"
ax.text(
    5,
    14.5,
    TITLE,
    ha="center",
    va="center",
    fontsize=14,
    fontweight="bold",
    family=FONT,
)
ax.text(
    5,
    14.05,
    "Smart Factory Monitoring System",
    ha="center",
    va="center",
    fontsize=10,
    style="italic",
    color="#555",
    family=FONT,
)


def box(y, text, face, edge, tcolor="black", h=0.95, w=5.4, x=5, sub=None):
    cx = x - w / 2
    patch = FancyBboxPatch(
        (cx, y - h / 2),
        w,
        h,
        boxstyle="round,pad=0.02,rounding_size=0.12",
        linewidth=1.8,
        edgecolor=edge,
        facecolor=face,
    )
    ax.add_patch(patch)
    if sub:
        ax.text(
            x,
            y + 0.16,
            text,
            ha="center",
            va="center",
            fontsize=10.5,
            fontweight="bold",
            color=tcolor,
            family=FONT,
        )
        ax.text(
            x,
            y - 0.22,
            sub,
            ha="center",
            va="center",
            fontsize=8.2,
            color=tcolor,
            family=FONT,
        )
    else:
        ax.text(
            x,
            y,
            text,
            ha="center",
            va="center",
            fontsize=10.5,
            fontweight="bold",
            color=tcolor,
            family=FONT,
        )
    return y


def arrow(y_from, y_to, label=None, x=5):
    patch = FancyArrowPatch(
        (x, y_from - 0.48),
        (x, y_to + 0.48),
        arrowstyle="-|>",
        mutation_scale=18,
        linewidth=1.8,
        color="#444",
    )
    ax.add_patch(patch)
    if label:
        ax.text(
            x + 0.25,
            (y_from + y_to) / 2,
            label,
            ha="left",
            va="center",
            fontsize=7.8,
            color="#666",
            style="italic",
            family=FONT,
        )


ys = [13.1, 11.85, 10.6, 9.35, 8.1, 6.85, 5.6, 4.35, 3.1]

box(ys[0], "IoT Sensors Simulation", "#eef3ff", "#3b6fb5", sub="10 virtual machines, 1 event/sec")
arrow(ys[0], ys[1])

box(ys[1], "Python Producer", "#eef3ff", "#3b6fb5", sub="generate + serialize JSON events")
arrow(ys[1], ys[2], "writes JSON")

box(ys[2], "Landing Zone (Volume)", "#f3f3f3", "#888", sub="raw JSON files, S3-like")
arrow(ys[2], ys[3])

box(ys[3], "Structured Streaming", "#e9f7ef", "#2e8b57", sub="readStream + checkpoints (exactly-once)")
arrow(ys[3], ys[4])

box(
    ys[4],
    "BRONZE Delta Table",
    "#f2d9b8",
    "#cd7f32",
    tcolor="#5a3a12",
    sub="raw events, append-only + metadata",
)
arrow(ys[4], ys[5])

box(
    ys[5],
    "SILVER Delta Table",
    "#e2e2e2",
    "#9a9a9a",
    tcolor="#333",
    sub="cleaned, validated, deduplicated, typed",
)
arrow(ys[5], ys[6])

box(
    ys[6],
    "Window Aggregations",
    "#e9f7ef",
    "#2e8b57",
    sub="tumbling window + watermark (event time)",
)
arrow(ys[6], ys[7])

box(
    ys[7],
    "GOLD Delta Table",
    "#fff1b8",
    "#d4af1a",
    tcolor="#5a4a00",
    sub="business metrics + alerts (overheat / vibration)",
)
arrow(ys[7], ys[8])

box(ys[8], "Dashboard / SQL Analytics", "#eef3ff", "#3b6fb5", sub="Databricks SQL, live KPIs")

# Invalid records dropped at Silver (not quarantined in v1)
qx = 9.1
qy = ys[5]
qp = FancyBboxPatch(
    (qx - 0.85, qy - 0.5),
    1.7,
    1.0,
    boxstyle="round,pad=0.02,rounding_size=0.1",
    linewidth=1.5,
    edgecolor="#b03030",
    facecolor="#fbe4e4",
    linestyle="--",
)
ax.add_patch(qp)
ax.text(
    qx,
    qy + 0.12,
    "Invalid",
    ha="center",
    va="center",
    fontsize=8.5,
    fontweight="bold",
    color="#8a1f1f",
    family=FONT,
)
ax.text(
    qx,
    qy - 0.2,
    "rows dropped",
    ha="center",
    va="center",
    fontsize=7.5,
    color="#8a1f1f",
    family=FONT,
)
qa = FancyArrowPatch(
    (5 + 2.7, qy),
    (qx - 0.85, qy),
    arrowstyle="-|>",
    mutation_scale=14,
    linewidth=1.4,
    color="#b03030",
    linestyle="--",
)
ax.add_patch(qa)
ax.text(
    (5 + 2.7 + qx - 0.85) / 2,
    qy + 0.22,
    "invalid",
    ha="center",
    va="center",
    fontsize=7,
    color="#b03030",
    style="italic",
    family=FONT,
)

lx = 0.9
ax.text(
    lx,
    qy + 0.45,
    "Medallion",
    ha="center",
    fontsize=8,
    fontweight="bold",
    color="#555",
    family=FONT,
    rotation=90,
)

fig.tight_layout()
png_path = os.path.join(HERE, "architecture.png")
svg_path = os.path.join(HERE, "architecture.svg")
fig.savefig(png_path, dpi=160, bbox_inches="tight", facecolor="white")
fig.savefig(svg_path, bbox_inches="tight", facecolor="white")
print("Saved:", png_path)
print("Saved:", svg_path)
