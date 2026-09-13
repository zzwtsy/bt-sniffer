#!/usr/bin/env python3
"""Check opaque sRGB color pairs, or the bundled Calm UI tokens.

Python 3.9+; standard library only. No network access and no file writes.
This is a small color calculator, not a browser or a WCAG conformance audit.
CSS mode supports only the two flat selector blocks in assets/tokens.css.
Formula source: W3C WCAG 2.2, Understanding Contrast (Minimum).
"""
from __future__ import annotations

import argparse
import math
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple


def parse_hex(value: str) -> Tuple[float, float, float]:
    """Return normalized sRGB channels for an opaque #RGB or #RRGGBB."""
    value = value.strip()
    if not re.fullmatch(r"#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})", value):
        raise ValueError(f"Unsupported color {value!r}; use opaque #RGB or #RRGGBB.")
    digits = value[1:]
    if len(digits) == 3:
        digits = "".join(character * 2 for character in digits)
    return tuple(int(digits[offset:offset + 2], 16) / 255.0
                 for offset in (0, 2, 4))  # type: ignore[return-value]


def luminance(value: str) -> float:
    channels = parse_hex(value)
    linear = [channel / 12.92 if channel <= 0.04045
              else ((channel + 0.055) / 1.055) ** 2.4
              for channel in channels]
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]


def contrast(foreground: str, background: str) -> float:
    first, second = sorted((luminance(foreground), luminance(background)))
    return (second + 0.05) / (first + 0.05)


def load_themes(path: Path) -> Dict[str, Dict[str, str]]:
    """Read only the documented flat CSS format; reject unsupported structure."""
    text = re.sub(r"/\*.*?\*/", "", path.read_text(encoding="utf-8"), flags=re.S)
    blocks = re.findall(r"([^{}]+)\{([^{}]*)\}", text)
    expected = {".calm-ui", '.calm-ui[data-theme="dark"]'}
    found: Dict[str, Dict[str, str]] = {}
    for selector, body in blocks:
        selector = selector.strip()
        if selector not in expected or selector in found:
            raise ValueError("CSS mode supports exactly one .calm-ui block and one "
                             '.calm-ui[data-theme="dark"] block; use explicit colors otherwise.')
        declarations = dict(re.findall(r"(--calm-[\w-]+)\s*:\s*([^;]+);", body))
        found[selector] = {key: value.strip() for key, value in declarations.items()}
    if set(found) != expected:
        raise ValueError("Required light/dark flat token blocks were not found.")
    if re.sub(r"[^{}]+\{[^{}]*\}", "", text).strip():
        raise ValueError("Unsupported content outside the two token blocks.")
    light = found[".calm-ui"]
    dark = {**light, **found['.calm-ui[data-theme="dark"]']}
    return {"light": light, "dark": dark}


def token_checks() -> List[Tuple[str, str, float]]:
    checks = []
    for background in ("bg", "surface", "surface-alt"):
        checks.extend((foreground, background, 4.5)
                      for foreground in ("text", "text-muted"))
    checks += [("on-accent", "accent", 4.5),
               ("on-accent", "accent-hover", 4.5),
               ("accent", "bg", 4.5),
               ("accent", "surface", 4.5),
               ("accent", "accent-soft", 4.5),
               ("text", "accent-soft", 4.5)]
    for background in ("bg", "surface", "surface-alt"):
        checks.extend((foreground, background, 3.0)
                      for foreground in ("border-control", "focus"))
    return checks


def print_check(label: str, foreground: str, background: str, minimum: float) -> bool:
    ratio = contrast(foreground, background)
    passed = ratio >= minimum  # Compare before rounding for display.
    print(f"{'PASS' if passed else 'FAIL'} {label}: "
          f"{foreground}/{background} = {ratio:.3f}:1 (minimum {minimum:g}:1)")
    return passed


def run_self_test() -> None:
    cases = [("#000", "#fff", 21.0), ("#FFFFFF", "#fff", 1.0),
             ("#F00", "#FF0000", 1.0)]
    for foreground, background, expected in cases:
        actual = contrast(foreground, background)
        if not math.isclose(actual, expected, abs_tol=1e-12):
            raise ValueError(f"Self-test failed: expected {expected}, got {actual}.")
    if not math.isclose(contrast("#123456", "#fedcba"),
                        contrast("#fedcba", "#123456"), abs_tol=1e-12):
        raise ValueError("Self-test failed: contrast must be symmetric.")
    for invalid in ("red", "#12", "#FFFF", "#12345678", "rgba(0,0,0,.5)", "#GGGGGG"):
        try:
            parse_hex(invalid)
        except ValueError:
            continue
        raise ValueError(f"Self-test failed: accepted invalid color {invalid!r}.")
    print("PASS self-tests: known ratios, short hex, symmetry, invalid inputs.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--css", type=Path, help="Bundled-format tokens.css path")
    parser.add_argument("--foreground", help="Opaque foreground #RGB or #RRGGBB")
    parser.add_argument("--background", help="Opaque background #RGB or #RRGGBB")
    parser.add_argument("--minimum", type=float, default=4.5, help="Explicit pair threshold (default 4.5)")
    parser.add_argument("--self-test", action="store_true", help="Run calculator self-tests only")
    args = parser.parse_args()
    explicit_pair = args.foreground is not None or args.background is not None
    if int(args.css is not None) + int(explicit_pair) + int(args.self_test) != 1:
        parser.error("Choose exactly one mode: --css, a foreground/background pair, or --self-test.")
    if explicit_pair and (args.foreground is None or args.background is None):
        parser.error("--foreground and --background must be provided together.")
    if not math.isfinite(args.minimum) or not 1.0 <= args.minimum <= 21.0:
        parser.error("--minimum must be a finite value between 1 and 21.")
    if not explicit_pair and args.minimum != 4.5:
        parser.error("--minimum applies only to explicit foreground/background pairs.")
    try:
        if args.self_test:
            run_self_test()
            return 0
        if explicit_pair:
            return 0 if print_check("explicit pair", args.foreground,
                                    args.background, args.minimum) else 1
        themes = load_themes(args.css)
        results = []
        for theme, tokens in themes.items():
            for foreground, background, minimum in token_checks():
                first = tokens[f"--calm-{foreground}"]
                second = tokens[f"--calm-{background}"]
                results.append(print_check(f"{theme} {foreground}/{background}",
                                           first, second, minimum))
        print(f"\n{sum(results)}/{len(results)} configured pairs passed. "
              "This does not establish UI accessibility conformance.")
        return 0 if all(results) else 1
    except (OSError, UnicodeError, ValueError, KeyError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
