#!/usr/bin/env python3
"""Generate ETL Test Data Generator Skill Architecture Diagram — v2 refined."""

from PIL import Image, ImageDraw, ImageFont
import os
import math

# === CONFIG ===
W, H = 2400, 1800
BG = "#0D1117"
CARD_BG = "#161B22"
CARD_BORDER = "#30363D"
ACCENT_BLUE = "#58A6FF"
ACCENT_GREEN = "#3FB950"
ACCENT_ORANGE = "#D29922"
ACCENT_PURPLE = "#BC8CFF"
ACCENT_RED = "#F85149"
ACCENT_CYAN = "#56D4DD"
TEXT_PRIMARY = "#E6EDF3"
TEXT_SECONDARY = "#8B949E"
TEXT_DIM = "#484F58"
ARROW_COLOR = "#58A6FF"
PHASE_COLORS = [ACCENT_BLUE, ACCENT_GREEN, ACCENT_ORANGE]

FONT_DIR = "/Users/portz/.claude/skills/canvas-design/canvas-fonts"


def load_font(name, size):
    path = os.path.join(FONT_DIR, name)
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()


# Fonts
font_title = load_font("WorkSans-Bold.ttf", 44)
font_subtitle = load_font("InstrumentSans-Regular.ttf", 22)
font_heading = load_font("WorkSans-Bold.ttf", 24)
font_heading_sm = load_font("WorkSans-Bold.ttf", 19)
font_body = load_font("InstrumentSans-Regular.ttf", 17)
font_body_sm = load_font("InstrumentSans-Regular.ttf", 15)
font_mono = load_font("JetBrainsMono-Regular.ttf", 14)
font_mono_bold = load_font("JetBrainsMono-Bold.ttf", 14)
font_mono_lg = load_font("JetBrainsMono-Bold.ttf", 16)
font_label = load_font("InstrumentSans-Bold.ttf", 14)
font_tag = load_font("InstrumentSans-Bold.ttf", 12)

img = Image.new("RGB", (W, H), BG)
draw = ImageDraw.Draw(img)


def rounded_rect(x, y, w, h, r, fill=None, outline=None, width=1):
    draw.rounded_rectangle(
        [x, y, x + w, y + h], radius=r, fill=fill, outline=outline, width=width
    )


def draw_arrow_h(x1, y1, x2, y2, color=ARROW_COLOR, width=2, dashed=False):
    """Draw an arrow with optional dashes."""
    if dashed:
        dx, dy = x2 - x1, y2 - y1
        length = math.sqrt(dx * dx + dy * dy)
        if length == 0:
            return
        dash_len, gap_len = 8, 6
        steps = int(length / (dash_len + gap_len))
        for i in range(steps):
            t1 = i * (dash_len + gap_len) / length
            t2 = min((i * (dash_len + gap_len) + dash_len) / length, 1.0)
            draw.line(
                [(x1 + dx * t1, y1 + dy * t1), (x1 + dx * t2, y1 + dy * t2)],
                fill=color,
                width=width,
            )
    else:
        draw.line([(x1, y1), (x2, y2)], fill=color, width=width)
    # Arrowhead
    angle = math.atan2(y2 - y1, x2 - x1)
    s = 10
    draw.polygon(
        [
            (x2, y2),
            (x2 - s * math.cos(angle - 0.4), y2 - s * math.sin(angle - 0.4)),
            (x2 - s * math.cos(angle + 0.4), y2 - s * math.sin(angle + 0.4)),
        ],
        fill=color,
    )


def draw_tag(x, y, text, color):
    tw = draw.textlength(text, font=font_tag)
    pad = 8
    r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
    bg_color = f"#{r // 4:02x}{g // 4:02x}{b // 4:02x}"
    rounded_rect(x, y, tw + pad * 2, 22, 4, fill=bg_color, outline=color, width=1)
    draw.text((x + pad, y + 3), text, fill=color, font=font_tag)
    return tw + pad * 2


def text_center(x, y, w, text, font, fill):
    tw = draw.textlength(text, font=font)
    draw.text((x + (w - tw) / 2, y), text, fill=fill, font=font)


# ================================================================
# TITLE
# ================================================================
draw.text((60, 35), "ETL Test Data Generator", fill=TEXT_PRIMARY, font=font_title)
draw.text(
    (60, 88),
    "Skill Architecture  |  Agent + Script Hybrid  |  Three-Phase Workflow",
    fill=TEXT_SECONDARY,
    font=font_subtitle,
)
draw.line([(60, 128), (W - 60, 128)], fill=CARD_BORDER, width=1)

# ================================================================
# ROW 1: Responsibility Split + Three Phases (horizontal)
# ================================================================
ROW1_Y = 155
ROW1_H = 200

# --- Responsibility Split ---
rp_x, rp_w = 60, 380
rounded_rect(rp_x, ROW1_Y, rp_w, ROW1_H, 10, fill=CARD_BG, outline=CARD_BORDER)
draw.text(
    (rp_x + 20, ROW1_Y + 14),
    "Responsibility Split",
    fill=TEXT_PRIMARY,
    font=font_heading,
)
draw.line(
    [(rp_x + 20, ROW1_Y + 48), (rp_x + rp_w - 20, ROW1_Y + 48)],
    fill=CARD_BORDER,
    width=1,
)

ay = ROW1_Y + 60
draw_tag(rp_x + 20, ay, "AI AGENT", ACCENT_PURPLE)
draw.text(
    (rp_x + 112, ay + 3), "Semantic Analysis", fill=TEXT_SECONDARY, font=font_body_sm
)
for i, t in enumerate(
    [
        "Analyze unresolved rules",
        "Choose transformer type",
        "Set parameters / generate new",
    ]
):
    draw.text(
        (rp_x + 30, ay + 28 + i * 20), f"·  {t}", fill=TEXT_DIM, font=font_body_sm
    )

sy = ay + 92
draw_tag(rp_x + 20, sy, "SCRIPTS", ACCENT_GREEN)
draw.text(
    (rp_x + 112, sy + 3),
    "Deterministic Compute",
    fill=TEXT_SECONDARY,
    font=font_body_sm,
)
for i, t in enumerate(["Parse rules / generate data", "Execute transforms / validate"]):
    draw.text(
        (rp_x + 30, sy + 28 + i * 20), f"·  {t}", fill=TEXT_DIM, font=font_body_sm
    )

# --- Three Phase Cards (horizontal) ---
PHASE_START_X = 480
PHASE_GAP = 20
PHASE_W = 600
PHASE_H = ROW1_H
phase_card_w = (PHASE_W * 3 + PHASE_GAP * 2) // 3  # ~200 each... too small
# Better: use remaining width
avail_w = W - 60 - PHASE_START_X
phase_card_w = (avail_w - PHASE_GAP * 2) // 3

phase_names = [
    "Phase 1: Rule Analysis",
    "Phase 2: Source Data Gen",
    "Phase 3: Expected Output",
]
phase_scripts = ["rule_parser.py", "data_generator.py", "expected_generator.py"]
phase_inputs = ["rules.csv", "rule_config.json", "config + source"]
phase_outputs = ["rule_config.json", "generated_source.txt", "generated_expected.txt"]
phase_descs = [
    [
        "Auto-parse ~80-90% of rules",
        "Agent resolves unresolved",
        "Validate final config",
    ],
    [
        "Faker generates test data",
        "Respects field_metadata",
        "Ensures conditional_coverage",
    ],
    [
        "Apply transformation_rules",
        "Execute transform pipeline",
        "Output target-format records",
    ],
]

for i in range(3):
    color = PHASE_COLORS[i]
    px = PHASE_START_X + i * (phase_card_w + PHASE_GAP)
    py = ROW1_Y

    rounded_rect(
        px, py, phase_card_w, PHASE_H, 10, fill=CARD_BG, outline=color, width=2
    )

    # Phase number circle
    cx, cy = px + 24, py + 24
    draw.ellipse([cx - 14, cy - 14, cx + 14, cy + 14], fill=color)
    text_center(cx - 14, cy - 11, 28, str(i + 1), font_heading_sm, "#0D1117")

    # Title
    draw.text(
        (px + 46, py + 12), phase_names[i], fill=TEXT_PRIMARY, font=font_heading_sm
    )

    # Script tag
    sw = draw.textlength(phase_scripts[i], font=font_mono_bold)
    rounded_rect(
        px + 20, py + 42, sw + 16, 22, 4, fill="#1C2333", outline=color, width=1
    )
    draw.text((px + 28, py + 44), phase_scripts[i], fill=color, font=font_mono_bold)

    # IN/OUT labels
    io_y = py + 42
    io_rx = px + 28 + sw + 24
    draw.text((io_rx, io_y), "IN:", fill=TEXT_DIM, font=font_label)
    draw.text((io_rx + 24, io_y), phase_inputs[i], fill=ACCENT_CYAN, font=font_mono)
    draw.text((io_rx, io_y + 18), "OUT:", fill=TEXT_DIM, font=font_label)
    draw.text(
        (io_rx + 34, io_y + 18), phase_outputs[i], fill=ACCENT_GREEN, font=font_mono
    )

    # Description bullets
    for j, desc in enumerate(phase_descs[i]):
        draw.text(
            (px + 20, py + 90 + j * 22),
            f"→  {desc}",
            fill=TEXT_SECONDARY,
            font=font_body_sm,
        )

    # Arrows between phases
    if i < 2:
        ax1 = px + phase_card_w + 2
        ax2 = px + phase_card_w + PHASE_GAP - 2
        arrow_y = py + PHASE_H // 2
        draw_arrow_h(ax1, arrow_y, ax2, arrow_y, color=TEXT_DIM, width=2)


# ================================================================
# ROW 2: Transform Engine (full width)
# ================================================================
ROW2_Y = ROW1_Y + ROW1_H + 30
eng_x, eng_w, eng_h = 60, W - 120, 280

rounded_rect(
    eng_x, ROW2_Y, eng_w, eng_h, 10, fill=CARD_BG, outline=ACCENT_ORANGE, width=2
)
draw.text(
    (eng_x + 20, ROW2_Y + 14), "Transform Engine", fill=TEXT_PRIMARY, font=font_heading
)
draw.text(
    (eng_x + 240, ROW2_Y + 18),
    "transform_engine.py",
    fill=ACCENT_ORANGE,
    font=font_mono_lg,
)
draw.line(
    [(eng_x + 20, ROW2_Y + 50), (eng_x + eng_w - 20, ROW2_Y + 50)],
    fill=CARD_BORDER,
    width=1,
)

# --- Built-in Transformers (left half) ---
bt_x = eng_x + 20
bt_y = ROW2_Y + 62
draw.text(
    (bt_x, bt_y), "Built-in Transformers", fill=TEXT_SECONDARY, font=font_heading_sm
)

builtins = [
    ("direct", "Copy source field directly"),
    ("fixed", "Fixed constant value"),
    ("empty", "Empty string output"),
    ("conditional_map", "Value-based mapping with default"),
    ("substring", "Extract substring by position"),
    ("phone_parser", "Parse area code / number"),
    ("composite", "Chain multiple transformers in pipeline"),
]

col_w = 340
for idx, (name, desc) in enumerate(builtins):
    col = idx // 4
    row = idx % 4
    x = bt_x + col * col_w
    y = bt_y + 30 + row * 30
    nw = draw.textlength(name, font=font_mono_bold)
    rounded_rect(x, y, nw + 12, 24, 4, fill="#1A1F2B", outline=CARD_BORDER)
    draw.text((x + 6, y + 4), name, fill=ACCENT_ORANGE, font=font_mono_bold)
    draw.text((x + nw + 22, y + 4), desc, fill=TEXT_DIM, font=font_body_sm)

# --- Plugin System (right half) ---
plugin_x = eng_x + 740
plugin_y = ROW2_Y + 62
draw.line(
    [(plugin_x - 20, ROW2_Y + 56), (plugin_x - 20, ROW2_Y + eng_h - 20)],
    fill=CARD_BORDER,
    width=1,
)

draw.text(
    (plugin_x, plugin_y), "Plugin System", fill=TEXT_SECONDARY, font=font_heading_sm
)
draw.text(
    (plugin_x + 160, plugin_y + 2),
    "custom_transformers/",
    fill=ACCENT_PURPLE,
    font=font_mono,
)
draw.text(
    (plugin_x + 360, plugin_y + 2),
    "auto-load at startup",
    fill=ACCENT_GREEN,
    font=font_label,
)

plugins = [
    ("delimiter_split", "Split by delimiter + select part (e.g., LAST,FIRST → FIRST)"),
    ("date_format", "Convert between date formats (e.g., MMDDYYYY → YYYY-MM-DD)"),
]
for idx, (name, desc) in enumerate(plugins):
    x = plugin_x
    y = plugin_y + 34 + idx * 34
    nw = draw.textlength(name, font=font_mono_bold)
    rounded_rect(x, y, nw + 12, 24, 4, fill="#1A1F2B", outline=ACCENT_PURPLE)
    draw.text((x + 6, y + 4), name, fill=ACCENT_PURPLE, font=font_mono_bold)
    draw.text((x + nw + 22, y + 4), desc, fill=TEXT_DIM, font=font_body_sm)

# Subagent box
gen_y = plugin_y + 110
gen_w = eng_w - (plugin_x - eng_x) - 30
rounded_rect(
    plugin_x, gen_y, gen_w, 80, 8, fill="#1C1A2E", outline=ACCENT_PURPLE, width=1
)
draw.text(
    (plugin_x + 16, gen_y + 10),
    "Subagent Transformer Generation",
    fill=ACCENT_PURPLE,
    font=font_heading_sm,
)
draw.text(
    (plugin_x + 16, gen_y + 34),
    "When no existing transformer matches → subagent generates new one",
    fill=TEXT_SECONDARY,
    font=font_body_sm,
)
draw.text(
    (plugin_x + 16, gen_y + 54),
    "→ Saved permanently in custom_transformers/ for automatic reuse",
    fill=TEXT_DIM,
    font=font_body_sm,
)

# Rule engine usage arrow (connects Phase 3 → Engine)
# Small annotation
ann_x = eng_x + eng_w - 260
ann_y = ROW2_Y + 14
draw.text(
    (ann_x, ann_y),
    "Used by Phase 3 (expected_generator)",
    fill=TEXT_DIM,
    font=font_body_sm,
)
draw_arrow_h(
    ann_x - 10, ann_y + 8, ann_x - 40, ann_y + 8, color=TEXT_DIM, width=1, dashed=True
)


# ================================================================
# ROW 3: Four-Layer Fallback
# ================================================================
ROW3_Y = ROW2_Y + eng_h + 30
fb_x, fb_w, fb_h = 60, W - 120, 130

rounded_rect(fb_x, ROW3_Y, fb_w, fb_h, 10, fill=CARD_BG, outline=CARD_BORDER)
draw.text(
    (fb_x + 20, ROW3_Y + 14),
    "Unresolved Rule Resolution — Four-Layer Fallback",
    fill=TEXT_PRIMARY,
    font=font_heading,
)

layers = [
    ("1", "Match Existing", "Use built-in or\nplugin transformer", ACCENT_GREEN),
    ("2", "Composite", "Chain multiple\ntransformers", ACCENT_BLUE),
    ("3", "Generate New", "Subagent creates\ncustom transformer", ACCENT_PURPLE),
    ("4", "Save & Reuse", "Auto-loaded from\ncustom_transformers/", ACCENT_ORANGE),
]

usable_w = fb_w - 80
layer_block_w = usable_w // 4
layer_y = ROW3_Y + 52

for idx, (num, title, desc, color) in enumerate(layers):
    lx = fb_x + 40 + idx * layer_block_w
    # Circle
    draw.ellipse([lx, layer_y, lx + 28, layer_y + 28], fill=color)
    text_center(lx, layer_y + 2, 28, num, font_heading_sm, "#0D1117")
    # Title
    draw.text((lx + 36, layer_y), title, fill=TEXT_PRIMARY, font=font_heading_sm)
    # Desc
    for k, line in enumerate(desc.split("\n")):
        draw.text(
            (lx + 36, layer_y + 24 + k * 18), line, fill=TEXT_DIM, font=font_body_sm
        )
    # Arrow
    if idx < 3:
        ax = lx + layer_block_w - 30
        draw_arrow_h(
            ax,
            layer_y + 14,
            ax + 24,
            layer_y + 14,
            color=TEXT_DIM,
            width=2,
            dashed=True,
        )


# ================================================================
# ROW 4: Directory Structure + Data Flow
# ================================================================
ROW4_Y = ROW3_Y + fb_h + 30
ROW4_H = 420

# --- Directory Structure (left) ---
dir_x, dir_w = 60, 780
rounded_rect(dir_x, ROW4_Y, dir_w, ROW4_H, 10, fill=CARD_BG, outline=CARD_BORDER)
draw.text(
    (dir_x + 20, ROW4_Y + 14),
    "Directory Structure",
    fill=TEXT_PRIMARY,
    font=font_heading,
)
draw.line(
    [(dir_x + 20, ROW4_Y + 48), (dir_x + dir_w - 20, ROW4_Y + 48)],
    fill=CARD_BORDER,
    width=1,
)

tree_lines = [
    ("skills/etl-test-data-generator/", "", True),
    ("  ├── SKILL.md", "Workflow guide (Agent reads)", False),
    ("  ├── scripts/", "", False),
    ("  │   ├── rule_parser.py", "Phase 1: Rule pre-parsing", False),
    ("  │   ├── transform_engine.py", "Core engine + validation", False),
    ("  │   ├── data_generator.py", "Phase 2: Faker data gen", False),
    ("  │   ├── expected_generator.py", "Phase 3: Expected output", False),
    ("  │   └── custom_transformers/", "Auto-loaded plugins", False),
    ("  │       ├── delimiter_split_transformer.py", "", False),
    ("  │       └── date_format_transformer.py", "", False),
    ("  └── reference/", "", False),
    ("      ├── rule_config_schema.json", "JSON Schema", False),
    ("      ├── example_rule_config.json", "", False),
    ("      └── example_rules.csv", "", False),
    ("", "", False),
    ("case/{case_name}/", "Input per case", True),
    ("  ├── rules.csv", "ETL mapping rules", False),
    ("  └── source.csv / expected.txt", "Reference files", False),
    ("", "", False),
    ("output/{case_name}/", "Generated output", True),
    ("  ├── rule_config.json", "Final configuration", False),
    ("  ├── generated_source.txt", "Test source data", False),
    ("  └── generated_expected.txt", "Expected results", False),
]

ty = ROW4_Y + 60
for path, comment, is_root in tree_lines:
    if not path:
        ty += 6
        continue
    tx = dir_x + 24
    draw.text(
        (tx, ty), path, fill=ACCENT_BLUE if is_root else TEXT_SECONDARY, font=font_mono
    )
    if comment:
        cw = draw.textlength(path, font=font_mono)
        draw.text((tx + cw + 16, ty), comment, fill=TEXT_DIM, font=font_body_sm)
    ty += 18


# --- Data Flow (right) ---
flow_x = dir_x + dir_w + 20
flow_w = W - 60 - flow_x
rounded_rect(flow_x, ROW4_Y, flow_w, ROW4_H, 10, fill=CARD_BG, outline=CARD_BORDER)
draw.text(
    (flow_x + 20, ROW4_Y + 14),
    "End-to-End Data Flow",
    fill=TEXT_PRIMARY,
    font=font_heading,
)
draw.line(
    [(flow_x + 20, ROW4_Y + 48), (flow_x + flow_w - 20, ROW4_Y + 48)],
    fill=CARD_BORDER,
    width=1,
)

nodes = [
    ("rules.csv", "Input ETL mapping rules", ACCENT_BLUE, True),
    ("rule_parser.py", "Auto-parse → draft config (~80-90%)", TEXT_SECONDARY, False),
    ("AI Agent", "Resolve unresolved_rules", ACCENT_PURPLE, True),
    ("rule_config.json", "Complete validated configuration", ACCENT_GREEN, True),
    ("data_generator.py", "Faker → realistic source records", TEXT_SECONDARY, False),
    ("generated_source.txt", "Test source data file", ACCENT_ORANGE, True),
    (
        "expected_generator.py",
        "Apply transforms → expected output",
        TEXT_SECONDARY,
        False,
    ),
    ("generated_expected.txt", "Expected test results", ACCENT_GREEN, True),
]

flow_cy = ROW4_Y + 64
flow_cx = flow_x + flow_w // 2
node_w = flow_w - 80
node_h = 34
node_gap = 8

for idx, (name, desc, color, is_key) in enumerate(nodes):
    nx = flow_cx - node_w // 2
    ny = flow_cy + idx * (node_h + node_gap)

    bg = "#1C2333" if is_key else "#131820"
    border = color if is_key else CARD_BORDER
    bw = 2 if is_key else 1

    rounded_rect(nx, ny, node_w, node_h, 6, fill=bg, outline=border, width=bw)
    draw.text((nx + 14, ny + 8), name, fill=color, font=font_mono_bold)
    nw = draw.textlength(name, font=font_mono_bold)
    draw.text((nx + 20 + nw, ny + 9), desc, fill=TEXT_DIM, font=font_body_sm)

    # Arrow down
    if idx < len(nodes) - 1:
        draw_arrow_h(
            flow_cx,
            ny + node_h + 1,
            flow_cx,
            ny + node_h + node_gap - 1,
            color=TEXT_DIM,
            width=1,
        )

# Side annotation: transform_engine.py feeds into expected_generator
te_node_y = flow_cy + 6 * (node_h + node_gap) + node_h // 2
te_ann_x = flow_cx + node_w // 2 + 6
# Only draw if space allows
if te_ann_x + 10 < flow_x + flow_w - 10:
    pass  # Skip to avoid overflow — the engine connection is shown in Row 2


# ================================================================
# FOOTER
# ================================================================
footer_y = H - 50
draw.line([(60, footer_y), (W - 60, footer_y)], fill=CARD_BORDER, width=1)
draw.text(
    (60, footer_y + 14),
    "ETL Test Data Generator Skill",
    fill=TEXT_DIM,
    font=font_body_sm,
)
draw.text(
    (W - 360, footer_y + 14),
    "Agent-Script Hybrid  ·  No LLM/CrewAI Dependencies",
    fill=TEXT_DIM,
    font=font_body_sm,
)


# ================================================================
# SAVE
# ================================================================
output_path = "/Users/portz/js/agent-krystal/output/etl_skill_architecture.png"
img.save(output_path, "PNG", dpi=(144, 144))
print(f"Saved: {output_path}")
print(f"Size: {W}x{H}")
