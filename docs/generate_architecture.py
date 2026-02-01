import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Circle, Rectangle, FancyBboxPatch, FancyArrowPatch
import numpy as np

# Create figure with clean, museum-quality aesthetic
fig, ax = plt.subplots(figsize=(16, 10), dpi=150)
ax.set_xlim(0, 100)
ax.set_ylim(0, 60)
ax.axis("off")

# Color palette - sophisticated and restrained
colors = {
    "bg": "#FAFAFA",
    "agent": "#2C3E50",
    "agent_light": "#34495E",
    "sftp": "#27AE60",
    "api": "#E67E22",
    "flow": "#3498DB",
    "config": "#7F8C8D",
    "text": "#2C3E50",
    "accent": "#E74C3C",
}

# Background
fig.patch.set_facecolor(colors["bg"])

# Title - KRystal
ax.text(
    50,
    56,
    "KRYSTAL",
    fontsize=24,
    fontweight="bold",
    ha="center",
    va="center",
    color=colors["text"],
    fontfamily="sans-serif",
    alpha=0.9,
)
ax.text(
    50,
    53.5,
    "End-to-End Testing Framework",
    fontsize=10,
    ha="center",
    va="center",
    color=colors["text"],
    fontfamily="sans-serif",
    alpha=0.6,
    style="italic",
)

# Main workflow line (horizontal)
workflow_y = 35
ax.plot(
    [8, 92],
    [workflow_y, workflow_y],
    color=colors["flow"],
    linewidth=2,
    alpha=0.3,
    linestyle="-",
    zorder=1,
)

# Five Agents - positioned along the main workflow
agents = [
    ("1", "Data\nGenerator", 15, workflow_y),
    ("2", "SFTP\nOperator", 32, workflow_y),
    ("3", "API\nTrigger", 49, workflow_y),
    ("4", "Polling\nMonitor", 66, workflow_y),
    ("5", "Result\nValidator", 83, workflow_y),
]

for num, name, x, y in agents:
    # Agent circle
    circle = Circle(
        (x, y), 4, facecolor=colors["agent"], edgecolor="white", linewidth=3, zorder=10
    )
    ax.add_patch(circle)

    # Agent number
    ax.text(
        x,
        y + 0.5,
        num,
        fontsize=14,
        fontweight="bold",
        ha="center",
        va="center",
        color="white",
        zorder=11,
    )

    # Agent name below
    ax.text(
        x,
        y - 6,
        name,
        fontsize=8,
        ha="center",
        va="top",
        color=colors["text"],
        fontweight="medium",
        zorder=11,
    )

# Flow arrows between agents
for i in range(len(agents) - 1):
    x1, y1 = agents[i][2], agents[i][3]
    x2, y2 = agents[i + 1][2], agents[i + 1][3]
    arrow = FancyArrowPatch(
        (x1 + 4.2, y1),
        (x2 - 4.2, y2),
        arrowstyle="->",
        mutation_scale=20,
        linewidth=2,
        color=colors["flow"],
        alpha=0.8,
        zorder=5,
    )
    ax.add_patch(arrow)

# External Services - SFTP (top)
sftp_y = 48
ax.text(
    32,
    sftp_y + 6,
    "SFTP Server",
    fontsize=9,
    ha="center",
    va="center",
    color=colors["sftp"],
    fontweight="bold",
)

# SFTP box
sftp_box = FancyBboxPatch(
    (24, sftp_y),
    16,
    4,
    boxstyle="round,pad=0.1",
    facecolor=colors["sftp"],
    edgecolor="white",
    linewidth=2,
    alpha=0.9,
)
ax.add_patch(sftp_box)
ax.text(
    32,
    sftp_y + 2,
    "Port 2223",
    fontsize=8,
    ha="center",
    va="center",
    color="white",
    fontweight="bold",
)

# Connection from Data Generator to SFTP
ax.plot(
    [15, 15],
    [39, sftp_y],
    color=colors["sftp"],
    linewidth=1.5,
    alpha=0.5,
    linestyle="--",
    zorder=2,
)
ax.plot(
    [15, 28],
    [sftp_y, sftp_y],
    color=colors["sftp"],
    linewidth=1.5,
    alpha=0.5,
    linestyle="--",
    zorder=2,
)
ax.annotate(
    "",
    xy=(28, sftp_y),
    xytext=(24, sftp_y),
    arrowprops=dict(arrowstyle="->", color=colors["sftp"], alpha=0.5, lw=1.5),
)

# Connection from SFTP Operator to SFTP
ax.plot([32, 32], [39, sftp_y], color=colors["sftp"], linewidth=2, alpha=0.6, zorder=2)
ax.annotate(
    "",
    xy=(32, sftp_y + 4),
    xytext=(32, sftp_y - 0.5),
    arrowprops=dict(arrowstyle="<->", color=colors["sftp"], alpha=0.6, lw=2),
)

# External Services - API (bottom)
api_y = 18
ax.text(
    57.5,
    api_y - 4,
    "API Stub",
    fontsize=9,
    ha="center",
    va="center",
    color=colors["api"],
    fontweight="bold",
)

# API box
api_box = FancyBboxPatch(
    (49.5, api_y),
    16,
    4,
    boxstyle="round,pad=0.1",
    facecolor=colors["api"],
    edgecolor="white",
    linewidth=2,
    alpha=0.9,
)
ax.add_patch(api_box)
ax.text(
    57.5,
    api_y + 2,
    "Port 8000",
    fontsize=8,
    ha="center",
    va="center",
    color="white",
    fontweight="bold",
)

# Connection from API Trigger to API
ax.plot(
    [49, 57.5], [31, api_y + 4], color=colors["api"], linewidth=2, alpha=0.6, zorder=2
)
ax.annotate(
    "",
    xy=(57.5, api_y + 4),
    xytext=(53, api_y + 5),
    arrowprops=dict(arrowstyle="->", color=colors["api"], alpha=0.6, lw=2),
)

# Connection from Polling Monitor to API
ax.plot(
    [66, 57.5],
    [31, api_y + 4],
    color=colors["api"],
    linewidth=1.5,
    alpha=0.4,
    linestyle="--",
    zorder=2,
)

# Configuration Layer (top banner)
config_y = 56
config_box = FancyBboxPatch(
    (5, config_y - 2),
    90,
    3,
    boxstyle="round,pad=0.05",
    facecolor=colors["config"],
    edgecolor="none",
    alpha=0.15,
)
ax.add_patch(config_box)

ax.text(
    10,
    config_y - 0.5,
    "Configuration",
    fontsize=7,
    ha="left",
    va="center",
    color=colors["config"],
    fontweight="bold",
    alpha=0.8,
)

config_items = [".env (API Key + Proxy)", "services.yaml", "secrets.env (SFTP/API)"]
for i, item in enumerate(config_items):
    x_pos = 28 + i * 22
    item_box = FancyBboxPatch(
        (x_pos - 8, config_y - 1.8),
        18,
        2.2,
        boxstyle="round,pad=0.05",
        facecolor="white",
        edgecolor=colors["config"],
        linewidth=1,
        alpha=0.9,
    )
    ax.add_patch(item_box)
    ax.text(
        x_pos,
        config_y - 0.7,
        item,
        fontsize=6,
        ha="center",
        va="center",
        color=colors["text"],
        alpha=0.7,
    )

# Workflow description at bottom
workflow_desc = [
    ("CSV Generation", 15, 12),
    ("SFTP Upload", 32, 12),
    ("API Trigger", 49, 12),
    ("Status Polling", 66, 12),
    ("Result Validation", 83, 12),
]

for desc, x, y in workflow_desc:
    ax.text(
        x,
        y,
        desc,
        fontsize=6,
        ha="center",
        va="center",
        color=colors["text"],
        alpha=0.5,
        style="italic",
    )

# Add legend box at bottom right
legend_x = 75
legend_y = 7
legend_elements = [
    ("Agent", colors["agent"]),
    ("SFTP", colors["sftp"]),
    ("API", colors["api"]),
    ("Data Flow", colors["flow"]),
]

ax.text(
    legend_x,
    legend_y + 3,
    "Legend",
    fontsize=7,
    ha="left",
    va="center",
    color=colors["text"],
    fontweight="bold",
    alpha=0.7,
)

for i, (label, color) in enumerate(legend_elements):
    y_offset = legend_y + 1.5 - i * 1.5
    if label == "Data Flow":
        ax.plot(
            [legend_x, legend_x + 2],
            [y_offset, y_offset],
            color=color,
            linewidth=2,
            alpha=0.6,
        )
    else:
        if label == "Agent":
            circle = Circle(
                (legend_x + 1, y_offset),
                0.6,
                facecolor=color,
                edgecolor="white",
                linewidth=1,
            )
            ax.add_patch(circle)
        else:
            rect = Rectangle(
                (legend_x + 0.4, y_offset - 0.6),
                1.2,
                1.2,
                facecolor=color,
                edgecolor="white",
                linewidth=1,
            )
            ax.add_patch(rect)

    ax.text(
        legend_x + 3.5,
        y_offset,
        label,
        fontsize=6,
        ha="left",
        va="center",
        color=colors["text"],
        alpha=0.6,
    )

# Add subtle border
border = Rectangle(
    (2, 2), 96, 56, fill=False, edgecolor=colors["text"], linewidth=1, alpha=0.1
)
ax.add_patch(border)

# Add version info at bottom left
ax.text(
    5,
    4,
    "v1.0 | CrewAI Powered",
    fontsize=5,
    ha="left",
    va="center",
    color=colors["text"],
    alpha=0.4,
)

plt.tight_layout()
plt.savefig(
    "/Users/portz/js/agent-krystal/docs/krystal-architecture.png",
    dpi=150,
    bbox_inches="tight",
    facecolor=colors["bg"],
)
plt.close()

print("✅ Architecture diagram created: docs/krystal-architecture.png")
